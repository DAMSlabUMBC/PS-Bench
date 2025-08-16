-module(ps_bench_erlang_mqtt_adapter).
-behaviour(gen_server).

-include("ps_bench_config.hrl").

-define(SUBSCRIPTION_ETS_TABLE_NAME, current_subscriptions).

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
    erlang:spawn_link(list_to_atom(InterfaceName), start_link, [TestName, ClientName, self()]),
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
    gen_server:call(ServerReference, {connect}),
    {reply, ok, State};

handle_call(connect_clean, _From, State = #{server_reference := ServerReference}) ->
    gen_server:call(ServerReference, {connect_clean}),
    {reply, ok, State};

handle_call(reconnect, _From, State = #{server_reference := ServerReference}) ->
    gen_server:call(ServerReference, {reconnect}),
    {reply, ok, State};

handle_call({subscribe, Topics}, _From, State = #{server_reference := ServerReference, tid := Tid}) ->
    do_subscribe(Topics, ServerReference, Tid),
    {reply, ok, State};

handle_call({publish, Topic, Data, PubOpts}, _From, State = #{server_reference := ServerReference}) ->
    gen_server:call(ServerReference, {publish, #{}, Topic, Data, PubOpts}),
    {reply, ok, State};

handle_call({unsubcribe, Topics}, _From, State = #{server_reference := ServerReference, tid := Tid}) ->
    gen_server:call(ServerReference, {unsubcribe, #{}, Topics}),

    % Update topic list
    SubTopicList = ets:lookup(Tid, ServerReference),
    NewTopicList = lists:filter(fun({TopicName, _}) -> lists:keymember(TopicName, 1, Topics) end, SubTopicList),
    ets:insert(Tid, {ServerReference, NewTopicList}),

    {reply, ok, State};

handle_call(disconnect, _From, State = #{server_reference := ServerReference}) ->
    gen_server:call(ServerReference, disconnect),
    {reply, ok, State};

handle_call(_, _, State) ->
    % No other operations supported
    {noreply, State}.

% Calls to start the actual timer functions
handle_cast(start_client_loops, State = #{device_type := DeviceType, server_reference := ServerReference}) ->
    {ok, PubTaskRef} = start_publication_loop(DeviceType, ServerReference),
    {ok, DisconLoopTaskRef} = start_disconnection_loop(DeviceType, ServerReference),
    {ok, ReconLoopTaskRef} = start_reconnection_loop(DeviceType, ServerReference),
    {noreply, State#{pub_task := PubTaskRef, discon_task := DisconLoopTaskRef, recon_task := ReconLoopTaskRef}};

handle_cast(stop, State = #{server_reference := ServerReference, pub_task := PubTaskRef, discon_task := DisconLoopTaskRef, recon_task := ReconLoopTaskRef}) ->
    % Stop all loops
    timer:cancel(PubTaskRef),
    timer:cancel(DisconLoopTaskRef),
    timer:cancel(ReconLoopTaskRef),

    % Tell interface to stop
    gen_server:cast(ServerReference, stop),
    {noreply, State}.

handle_info({?CONNECTED_MSG, {TimeNs}, ClientName}, State) ->
    ps_bench_store:record_connect(ClientName, TimeNs),
    {noreply, State};

handle_info({?DISCONNECTED_MSG, {TimeNs, Reason}, ClientName}, State) ->
    case Reason of 
        normal ->
            % This is expected, don't record
            {noreply, State};
        _ ->
            ps_bench_store:record_disconnect(ClientName, TimeNs),
            {noreply, State}
    end;

handle_info({?PUBLISH_RECV_MSG, {RecvTimeNs, Topic, Payload}, ClientName}, State) ->
    % Extracted needed info and store
    Bytes = byte_size(Payload),
    {Seq, PubTimeNs, _Rest} = ps_bench_utils:decode_seq_header(Payload),
    ps_bench_store:record_recv(ClientName, Topic, Seq, PubTimeNs, RecvTimeNs, Bytes),
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

do_subscribe(Topics, ServerReference, Tid) ->
    ok = gen_server:call(ServerReference, {subscribe, #{}, Topics}),
    
    % Update topic list
    SubTopicList = ets:lookup(Tid, ServerReference),
    NewTopicList = lists:merge(Topics, SubTopicList),
    ets:insert(Tid, {ServerReference, NewTopicList}).

start_publication_loop(DeviceType, ServerReference) ->
    % Lookup needed parameters
    {ok, PubFrequencyMs} = ps_bench_config_manager:fetch_device_publication_frequency(DeviceType),
    {ok, PayloadSizeMean, PayloadSizeVariance} = ps_bench_config_manager:fetch_device_payload_info(DeviceType),

    % Construct topic
    DeviceTypeBinary = atom_to_binary(DeviceType, latin1),
    Topic = <<?MQTT_TOPIC_PREFIX/binary, DeviceTypeBinary/binary>>,

    % Create task
    {ok, QoS} = ps_bench_config_manager:fetch_mqtt_qos_for_device(DeviceType),
    timer:apply_interval(PubFrequencyMs, fun publication_loop/5, [ServerReference, Topic, QoS, PayloadSizeMean, PayloadSizeVariance]).

publication_loop(ServerReference, Topic, QoS, PayloadSizeMean, PayloadSizeVariance) ->
    Payload = ps_bench_utils:generate_payload_data(PayloadSizeMean, PayloadSizeVariance, Topic),
    gen_server:call(ServerReference, {publish, #{}, Topic, Payload, [{qos, QoS}]}). % TODO, more settings?

start_disconnection_loop(DeviceType, ServerReference) ->
    % Lookup needed parameters
    {ok, DisconPeriodMs, DisconChance} = ps_bench_config_manager:fetch_device_disconnect_info(DeviceType),

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
    {ok, ReconPeriodMs, ReconChance} = ps_bench_config_manager:fetch_device_reconnect_info(DeviceType),

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
            case gen_server:call(ServerReference, {reconnect}) of
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

terminate(normal, {TestName, ClientName}) -> % TODO Cleanup
    io:format("Client Shutdown ~s for Erlang Client ~s~n",[TestName, ClientName]),
    ok;

terminate(shutdown, {TestName, ClientName}) -> % TODO Cleanup
    io:format("Supervisor Shutdown ~s for Erlang Client ~s~n",[TestName, ClientName]),
    ok.

code_change(_OldVsn, State, _Extra) -> 
    {ok, State}.