-module(ps_bench_erlang_mqtt_adapter).
-behaviour(gen_server).

-include("ps_bench_config.hrl").

-define(SUBSCRIPTION_ETS_TABLE_NAME, current_mqtt_subscriptions).

% Interface currently needs to:
% - Accept handlers for recv, disconnect, connect
% - Not fail if the publication doesn't go through
% - Be noop if connect is called when already connected or disconnect is called when not connected
% - Return {ok, new_connection} or {ok, already_connected} for connect, reconnect, connect_clean

%% public
-export([start_link/4]).

%% gen_server callbacks
-export([init/1, handle_call/3,
         handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([publication_loop/5, disconnect_loop/2, reconnect_loop/2]).

start_link(TestName, InterfaceName, ClientName, DeviceType) ->
    gen_server:start_link(?MODULE, [TestName, InterfaceName, ClientName, DeviceType], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([TestName, InterfaceName, ClientName, DeviceType]) ->
    %% Normalize names
    ServerMod  = ps_bench_utils:convert_to_atom(InterfaceName),
    ServerName = ps_bench_utils:convert_to_atom(ClientName),

    %% Start the interface process and always use the registered name
    {ok, _Pid} = ServerMod:start_link(TestName, ServerName, self()),
    ServerRef   = ServerName,

    Tid = ensure_subs_table(),
    {ok, #{ test_name        => TestName
          , device_type      => DeviceType
          , server_reference => ServerRef
          , pub_task         => 0
          , discon_task      => 0
          , recon_task       => 0
          , tid              => Tid
          }}.

%% helpers 

ensure_subs_table() ->
    case ets:whereis(?SUBSCRIPTION_ETS_TABLE_NAME) of
        undefined -> ets:new(?SUBSCRIPTION_ETS_TABLE_NAME, [set, public, named_table]);
        Tid       -> Tid
    end.

% For direct commands, we just forward these messages to the actual client
handle_call(connect, _From, State = #{server_reference := ServerReference}) ->
    Reply = catch gen_server:call(ServerReference, connect),
    case Reply of
        {'EXIT', Reason} ->
            io:format("adapter: connect -> interface call failed: ~p~n", [Reason]),
            {reply, ok, State};  % swallow to keep orchestration alive
        _ ->
            {reply, ok, State}
    end;

handle_call(connect_clean, _From, State = #{server_reference := ServerReference}) ->
    gen_server:call(ServerReference, connect_clean),
    {reply, ok, State};

handle_call(reconnect, _From, State = #{server_reference := ServerReference}) ->
    gen_server:call(ServerReference, reconnect),
    {reply, ok, State};

handle_call(subscribe, _From, State = #{server_reference := ServerReference, tid := Tid}) ->
    do_subscribe([], ServerReference, Tid),
    {reply, ok, State};

handle_call({publish, Topic, Seq, Data, PubOpts}, _From, State = #{server_reference := ServerReference}) ->
    {ok, Result} = gen_server:call(ServerReference, {publish, #{}, Topic, Data, PubOpts}),
    case Result of 
        published ->
            ps_bench_store:record_publish(ServerReference, Topic, Seq);
        not_connected ->
            % This isn't an error, it just means we didn't publish
            ok
    end,
    {reply, ok, State};

handle_call({unsubscribe, Topics}, _From, State = #{server_reference := ServerReference, tid := Tid}) ->
    gen_server:call(ServerReference, {unsubscribe, #{}, Topics}),

    %% Update topic list to REMOVE the given topics
    case ets:lookup(Tid, ServerReference) of
        [] -> ok;
        [{_, SubTopicList}] ->
            Remaining =
                lists:filter(
                  fun({TopicName, _Opts}) ->
                    %% keep only those NOT in the unsub list
                    not lists:keymember(TopicName, 1, Topics)
                end,
                SubTopicList),
            ets:insert(Tid, {ServerReference, Remaining})
    end,
    {reply, ok, State};

handle_call(disconnect, _From, State = #{server_reference := ServerReference}) ->
    gen_server:call(ServerReference, disconnect),
    {reply, ok, State};

handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

% Calls to start the actual timer functions
handle_cast(start_client_loops, State = #{device_type := DeviceType, server_reference := ServerReference}) ->

    % Subscriber clients don't have client loops
    case DeviceType of
        subscriber ->
            {noreply, State};
        _ ->
            {ok, PubTaskRef} = start_publication_loop(DeviceType, ServerReference),
            {ok, DisconLoopTaskRef} = start_disconnection_loop(DeviceType, ServerReference),
            {ok, ReconLoopTaskRef} = start_reconnection_loop(DeviceType, ServerReference),
        {noreply, State#{pub_task := PubTaskRef, discon_task := DisconLoopTaskRef, recon_task := ReconLoopTaskRef}}
    end;

handle_cast(stop, State = #{server_reference := ServerReference, pub_task := PubTaskRef, discon_task := DisconLoopTaskRef, recon_task := ReconLoopTaskRef}) ->
    % Stop all loops
    timer:cancel(PubTaskRef),
    timer:cancel(DisconLoopTaskRef),
    timer:cancel(ReconLoopTaskRef),

    % Tell interface to stop
    gen_server:call(ServerReference, stop),
    {noreply, State}.

handle_info({?CONNECTED_MSG, {TimeNs}, ClientName}, State) ->
    ps_bench_store:record_connect(ClientName, TimeNs),
    {noreply, State};

handle_info({?DISCONNECTED_MSG, {TimeNs, Reason}, ClientName}, State) ->
    case Reason of 
        normal ->
            ps_bench_store:record_disconnect(ClientName, TimeNs, expected),
            {noreply, State};
        _ ->
            ps_bench_store:record_disconnect(ClientName, TimeNs, unexpected),
            {noreply, State}
    end;

handle_info({?PUBLISH_RECV_MSG, {RecvTimeNs, Topic, Payload}, ClientName}, State) ->
    % Extracted needed info and store
    Bytes = byte_size(Payload),
    
    % Try to decode with publisher ID
    {Seq, PubTimeNs, PublisherID, _Rest} = 
        case ps_bench_utils:decode_seq_header_with_publisher(Payload) of
            {S, T, P, R} -> {S, T, P, R};
            _ -> 
                % Fallback to old format
                {S, T, R} = ps_bench_utils:decode_seq_header(Payload),
                {S, T, unknown, R}
        end,
    
    % Updated call with PublisherID
    ps_bench_store:record_recv(ClientName, Topic, Seq, PubTimeNs, RecvTimeNs, Bytes, PublisherID),
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

do_subscribe(_Topics, ServerReference, Tid) ->

    % Currently only support wildcarded topic
    % We will subscribe to a topic for each QoS to support multi QoS deployments
    QoS0End = ps_bench_utils:convert_to_binary("qos_0/#"),
    QoS1End = ps_bench_utils:convert_to_binary("qos_1/#"),
    QoS2End = ps_bench_utils:convert_to_binary("qos_2/#"),
    QoS0Topic = <<?MQTT_TOPIC_PREFIX/binary, QoS0End/binary>>,
    QoS1Topic = <<?MQTT_TOPIC_PREFIX/binary, QoS1End/binary>>,
    QoS2Topic = <<?MQTT_TOPIC_PREFIX/binary, QoS2End/binary>>,
    NewTopics = [{QoS0Topic, [{qos, 0}]}, {QoS1Topic, [{qos, 1}]}, {QoS2Topic, [{qos, 2}]}],

    ok = gen_server:call(ServerReference, {subscribe, #{}, NewTopics}),
    PrevTopics =
        case ets:lookup(Tid, ServerReference) of
            []              -> [];
            [{_, TopicList}]-> TopicList
        end,
    %% keep unique topics
    NewTopicList = lists:usort(NewTopics ++ PrevTopics),
    ets:insert(Tid, {ServerReference, NewTopicList}).

start_publication_loop(DeviceType, ServerReference) ->
    % Lookup needed parameters
    {ok, PubFrequencyMs} = ps_bench_config_manager:fetch_device_publication_frequency(DeviceType),
    {ok, PayloadSizeMean, PayloadSizeVariance} = ps_bench_config_manager:fetch_device_payload_info(DeviceType),

    % Construct topic
    {ok, QoS} = ps_bench_config_manager:fetch_mqtt_qos_for_device(DeviceType),
    QoSPart = lists:flatten(io_lib:format("qos_~p/", [QoS])),
    QoSPartBinary = ps_bench_utils:convert_to_binary(QoSPart),
    DeviceTypeBinary = atom_to_binary(DeviceType, latin1),
    Topic = <<?MQTT_TOPIC_PREFIX/binary, QoSPartBinary/binary, DeviceTypeBinary/binary>>,

    % Create task
    {ok, TRef} =
    timer:apply_interval(
      PubFrequencyMs,
      ?MODULE, publication_loop,
      [ServerReference, Topic, QoS, PayloadSizeMean, PayloadSizeVariance]),
    {ok, TRef}.


publication_loop(ServerReference, Topic, QoS, PayloadSizeMean, PayloadSizeVariance) ->
    {Seq, Payload} = ps_bench_utils:generate_mqtt_payload_data(PayloadSizeMean, PayloadSizeVariance, Topic),
    {ok, Result} = gen_server:call(ServerReference, {publish, #{}, Topic, Payload, [{qos, QoS}]}),
    case Result of 
        published ->
            ps_bench_store:record_publish(ServerReference, Topic, Seq);
        not_connected ->
            % This isn't an error, it just means we didn't publish
            ok
    end.

start_disconnection_loop(DeviceType, ServerReference) ->
    % Lookup needed parameters
    {ok, DisconPeriodMs, DisconChance} = ps_bench_config_manager:fetch_device_disconnect_info(DeviceType),

    % Make sure we have a non-zero period
    case DisconPeriodMs of 
        Period when Period > 0 ->
            % apply_repeatedly doesn't run a new instance until the previous finished
            {ok, TRef} =
                timer:apply_repeatedly(
                    Period,
                    ?MODULE, disconnect_loop,
                    [ServerReference, DisconChance]),
            {ok, TRef};
        _ ->
            {ok, 0}
    end.

disconnect_loop(ServerReference, DisconChance) ->
    % Check to see if we should reconnect
    case ps_bench_utils:evaluate_uniform_chance(DisconChance) of
        true ->
            gen_server:call(ServerReference, disconnect);
        false ->
            ok
    end.

start_reconnection_loop(DeviceType, ServerReference) ->
    % Lookup needed parameters
    {ok, ReconPeriodMs, ReconChance} = ps_bench_config_manager:fetch_device_reconnect_info(DeviceType),

    % Make sure we have a non-zero period
    case ReconPeriodMs of 
        Period when Period > 0 ->
            {ok, TRef} =
                timer:apply_repeatedly(
                    Period,
                    ?MODULE, reconnect_loop,
                    [ServerReference, ReconChance]),
            {ok, TRef};
        _ ->
            {ok, 0}
    end.

reconnect_loop(ServerReference, ReconnectChance) ->
    % Check to see if we should reconnect
    case ps_bench_utils:evaluate_uniform_chance(ReconnectChance) of
        true ->
            case gen_server:call(ServerReference, reconnect) of
                {ok, new_connection} ->
                    % Now resubscribe to the needed topics
                    resub_to_previous_topics(ServerReference);
                _ ->
                    ok
            end;
        false ->
            ok
    end.

resub_to_previous_topics(ServerReference) ->
    case ets:whereis(?SUBSCRIPTION_ETS_TABLE_NAME) of 
        undefined ->
            ok;
        Tid ->
            case ets:take(Tid, ServerReference) of
                [] ->
                    ok;
                [{ServerReference, Topics}] ->
                    do_subscribe(Topics, ServerReference, Tid);
                _ ->
                    ok
            end
    end.

terminate(Reason, State) ->
    %% Optional: log a helpful line; avoid pattern mismatches.
    Test = maps:get(test_name, State, undefined),
    Ref  = maps:get(server_reference, State, undefined),
    io:format("Adapter terminating. reason=~p test=~p server_ref=~p~n",
              [Reason, Test, Ref]),
    ok.

code_change(_OldVsn, State, _Extra) -> 
    {ok, State}.