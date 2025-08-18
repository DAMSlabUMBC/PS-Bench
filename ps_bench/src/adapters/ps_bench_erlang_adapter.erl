-module(ps_bench_erlang_adapter).
-behaviour(gen_server).

-include("ps_bench_config.hrl").

-define(SUBSCRIPTION_ETS_TABLE_NAME, current_subscriptions).
-define(ERLANG_MQTT_MSG_HANDLERS, #{publish => fun handle_publish_event/1, 
                                        connected => fun handle_connect_event/1,
                                        disconnected => fun handle_disconnect_event/1}).

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

start_link(TestName, InterfaceName, ClientName, DeviceType) ->
    gen_server:start_link(?MODULE, [TestName, InterfaceName, ClientName, DeviceType], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([TestName, InterfaceName, ClientName, DeviceType]) ->

    % This function spawns the erlang process defined by the config
    erlang:spawn_link(list_to_atom(InterfaceName), start_link, [TestName, ClientName]),
    ServerReference = list_to_atom(ClientName),

    % Create or fetch the ETS table for subscriptions
    case ets:whereis(?SUBSCRIPTION_ETS_TABLE_NAME) of
        undefined ->
            Tid = ets:new(?SUBSCRIPTION_ETS_TABLE_NAME, [set, public, named_table]),
            {ok, #{test_name => TestName, device_type => DeviceType, server_reference => ServerReference, 
                pub_task => 0, discon_task => 0, recon_task => 0, tid => Tid}};
        Tid ->
            {ok, #{test_name => TestName, device_type => DeviceType, server_reference => ServerReference, 
                pub_task => 0, discon_task => 0, recon_task => 0, tid => Tid}}
    end.

% For direct commands, we just forward these messages to the actual client
handle_call(connect, _From, State = #{server_reference := ServerReference}) ->
    gen_server:call(ServerReference, {connect, ?ERLANG_MQTT_MSG_HANDLERS}),
    {reply, ok, State};

handle_call(connect_clean, _From, State = #{server_reference := ServerReference}) ->
    gen_server:call(ServerReference, {connect_clean, ?ERLANG_MQTT_MSG_HANDLERS}),
    {reply, ok, State};

handle_call(reconnect, _From, State = #{server_reference := ServerReference}) ->
    gen_server:call(ServerReference, {reconnect, ?ERLANG_MQTT_MSG_HANDLERS}),
    {reply, ok, State};

handle_call({subscribe, Topics}, _From, State = #{server_reference := ServerReference, tid := Tid}) ->
    do_subscribe(Topics, ServerReference, Tid),
    {reply, ok, State};

handle_call({publish, Topic, Data, PubOpts}, _From, State = #{server_reference := ServerReference}) ->
    gen_server:call(ServerReference, {publish, #{}, Topic, Data, PubOpts}),
    {reply, ok, State};

handle_call({unsubcribe, Topics}, _From, State = #{server_reference := ServerReference, tid := Tid}) ->
    gen_server:call(ServerReference, {unsubcribe, #{}, Topics}),

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

% Calls to start the actual timer functions
handle_cast(start_client_loops, State = #{device_type := DeviceType, server_reference := ServerReference}) ->
    {ok, PubTaskRef} = start_publication_loop(DeviceType, ServerReference),
    {ok, DisconLoopTaskRef} = start_disconnection_loop(DeviceType, ServerReference),
    {ok, ReconLoopTaskRef} = start_reconnection_loop(DeviceType, ServerReference),
    {noreply, State#{pub_task := PubTaskRef, discon_task := DisconLoopTaskRef, recon_task := ReconLoopTaskRef}};

handle_cast(stop, State = #{server_reference := ServerReference}) ->
    gen_server:cast(ServerReference, stop),
    {noreply, State}.

handle_call(_, _, State) ->
    {reply, ok, State}.

do_subscribe(Topics, ServerReference, Tid) ->
    ok = gen_server:call(ServerReference, {subscribe, #{}, Topics}),
    PrevTopics =
        case ets:lookup(Tid, ServerReference) of
            []              -> [];
            [{_, TopicList}]-> TopicList
        end,
    %% keep unique topics
    NewTopicList = lists:usort(Topics ++ PrevTopics),
    ets:insert(Tid, {ServerReference, NewTopicList}).

start_publication_loop(DeviceType, ServerReference) ->
    % Lookup needed parameters
    {ok, PubFrequencyMs} = ps_bench_config_manager:fetch_property_for_device(DeviceType, ?DEVICE_PUB_FREQ_PROP),
    {ok, PayloadSizeMean} = ps_bench_config_manager:fetch_property_for_device(DeviceType, ?DEVICE_SIZE_MEAN_PROP),
    {ok, PayloadSizeVariance} = ps_bench_config_manager:fetch_property_for_device(DeviceType, ?DEVICE_SIZE_VARIANCE_PROP),

    % Construct topic
    DeviceTypeBinary = atom_to_binary(DeviceType, latin1),
    Topic = <<?MQTT_TOPIC_PREFIX/binary, DeviceTypeBinary/binary>>,

    % Create task
    QoS = 0, % TODO: Get QoS from config
    timer:apply_interval(PubFrequencyMs, fun publication_loop/5, [ServerReference, Topic, QoS, PayloadSizeMean, PayloadSizeVariance]).

publication_loop(ServerReference, Topic, QoS, PayloadSizeMean, PayloadSizeVariance) ->
    Payload = ps_bench_utils:generate_payload(PayloadSizeMean, PayloadSizeVariance),
    gen_server:call(ServerReference, {publish, #{}, Topic, Payload, [{qos, QoS}]}). % TODO, more settings?

start_disconnection_loop(DeviceType, ServerReference) ->
    % Lookup needed parameters
    {ok, DisconPeriodMs} = ps_bench_config_manager:fetch_property_for_device(DeviceType, ?DEVICE_DISCON_CHECK_MS_PROP),
    {ok, DisconChance} = ps_bench_config_manager:fetch_property_for_device(DeviceType, ?DEVICE_DISCON_PCT_PROP),

    % Make sure we have a non-zero period
    case DisconPeriodMs of 
        Period when Period > 0 ->
            % apply_repeatedly doesn't run a new instance until the previous finished
            timer:apply_repeatedly(Period, fun disconnect_loop/2, [ServerReference, DisconChance]);
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
    {ok, ReconPeriodMs} = ps_bench_config_manager:fetch_property_for_device(DeviceType, ?DEVICE_RECON_CHECK_MS_PROP),
    {ok, ReconChance} = ps_bench_config_manager:fetch_property_for_device(DeviceType, ?DEVICE_RECON_PCT_PROP),

    % Make sure we have a non-zero period
    case ReconPeriodMs of 
        Period when Period > 0 ->
            % apply_repeatedly doesn't run a new instance until the previous finished
            timer:apply_repeatedly(Period, fun reconnect_loop/2, [ServerReference, ReconChance]);
        _ ->
            {ok, 0}
    end.

reconnect_loop(ServerReference, ReconnectChance) ->
    % Check to see if we should reconnect
    case ps_bench_utils:evaluate_uniform_chance(ReconnectChance) of
        true ->
            case gen_server:call(ServerReference, {reconnect, ?ERLANG_MQTT_MSG_HANDLERS}) of
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
    Tid = ets:whereis(?SUBSCRIPTION_ETS_TABLE_NAME),
    case ets:take(Tid, ServerReference) of
        [] ->
            ok;
        [{ServerReference, Topics}] ->
            do_subscribe(Topics, ServerReference, Tid)
    end.

handle_connect_event(Properties) ->
    io:format("Recv Connect with ~p~n",[Properties]),
    ok.

handle_publish_event(Msg) when is_map(Msg) ->
    TRecvNs = erlang:monotonic_time(nanosecond),
    Payload = maps:get(payload, Msg, <<>>),
    TopicBin = maps:get(topic,   Msg, <<"unknown">>),
    Bytes    = byte_size(Payload),
    ok = ps_bench_store:record_recv(TopicBin, undefined, undefined, TRecvNs, Bytes),
    ok.


handle_disconnect_event(Arg) ->
    io:format("Recv Disconnect with ~p~n",[Arg]),
    ok.

terminate(normal, {TestName, ClientName}) -> % TODO Cleanup
    io:format("Client Shutdown ~s for Erlang Client ~s~n",[TestName, ClientName]),
    ok;

terminate(shutdown, {TestName, ClientName}) -> % TODO Cleanup
    io:format("Supervisor Shutdown ~s for Erlang Client ~s~n",[TestName, ClientName]),
    ok.

code_change(_OldVsn, State, _Extra) -> 
    {ok, State}.