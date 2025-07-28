-module(ps_bench_default_erlang_client).
-behaviour(gen_server).

-include("ps_bench_config.hrl").

%% public
-export([start_link/3]).

%% gen_server callbacks
-export([init/1, handle_call/3,
         handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

start_link(TestName, ClientName, DeviceType) ->
    gen_server:start_link(?MODULE, [{TestName, ClientName, DeviceType}], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([{TestName, ClientName, DeviceType}]) ->
    {ok, #{test_name => TestName, client_name => ClientName, client_pid => 0, device_type => DeviceType, connected => false, first_start => true}}.

handle_call(connect, _From, State = #{first_start := FirstStart}) ->
    % If not specifically told, we start clean by default, then preserve old sessions on reconnects
    do_connect(FirstStart, State);

handle_call(connect_clean, _From, State) ->
    % Force clean connect
    do_connect(false, State);

handle_call(reconnect, _From, State) ->
    % Force restablishment of session
    % By the MQTT standard, this just starts a clean session if none existed previously
    do_connect(true, State);

handle_call({subscribe, Properties, Topics}, _From, State = #{client_pid := ClientPid}) ->
    % Subscribe with the on Topic with Options
    {ok, _Props, _ReasonCodes} = emqtt:subscribe(ClientPid, Properties, Topics),
    {reply, ok, State};

handle_call({publish, Properties, Topic, Data, PubOpts}, _From, State = #{client_pid := ClientPid}) when is_binary(Topic), is_binary(Data) ->
    % Need to check QoS
    case lists:keysearch(qos, 1, PubOpts) of
        % QoS >= 1, we need to return PacketId
        {value, {qos, QoS}} when QoS >= 1  -> 
            {ok, PacketId} = emqtt:publish(ClientPid, Topic, Properties, Data, PubOpts),
            {reply, {ok, PacketId}, State};
        % QoS wasn't specified or was given a value of 0
        _ ->
            ok = emqtt:publish(ClientPid, Topic, Properties, Data, PubOpts),
            {reply, ok, State}
    end;
    

handle_call({unsubcribe, Properties, Topics}, _From, State = #{client_pid := ClientPid}) ->
    % Subscribe with the on Topic with Options
    {ok, _Props, _ReasonCodes} = emqtt:unsubscribe(ClientPid, Properties, Topics),
    {reply, ok, State};

handle_call(disconnect, _From, State = #{client_pid := ClientPid, connected := Connected}) ->
    % Disconnect from MQTT broker if connected
    case Connected of 
        true ->
            % For now, we assume the properties are accepted as requested
            ok = emqtt:disconnect(ClientPid),
            {reply, ok, State#{client_pid := 0, connected := false}};
        false ->
            % Not connected don't do anything
            {reply, ok, State}
    end;

handle_call(_, _, State) ->
    % Subscribe with the on Topic with Options
    {noreply, State}.

handle_cast(stop, State = #{client_pid := ClientPid}) ->
    % Shutdown the client
    ok = emqtt:stop(ClientPid),
    {noreply, State}.

handle_info(_Info, State) ->
    %% no info logic yet, just fulfill behaviour
    {noreply, State}.

% Message handlers
handle_connect(Properties) ->
    io:format("Recv Connect with ~p~n",[Properties]),
    ok.

handle_publish(_Msg = #{qos := _QoS, properties := _Props, payload := _Payload}) ->
    io:format("Recv Publish with ~p~n",[_Msg]),
    ok.

handle_disconnect(Arg) ->
    io:format("Recv Disconnect with ~p~n",[Arg]),
    ok.

terminate(normal, {TestName, ClientName}) -> 
    io:format("Client Shutdown ~s for Erlang Client ~s~n",[TestName, ClientName]),
    ok;

terminate(shutdown, {TestName, ClientName}) ->
    io:format("Supervisor Shutdown ~s for Erlang Client ~s~n",[TestName, ClientName]),
    ok.

code_change(_OldVsn, State, _Extra) -> 
    {ok, State}.

start_client_link(ClientName, CleanStart) ->
    
    {ok, BrokerIP} = ps_bench_config_manager:fetch_protocol_property(?MQTT_BROKER_IP_PROP),
    {ok, BrokerPort} = ps_bench_config_manager:fetch_protocol_property(?MQTT_BROKER_PORT_PROP),
    {ok, Protocol} = ps_bench_config_manager:fetch_protocol_type(),

    % Configure properties
    PropList = [
        {owner, self()}, % This process should get notifications from the MQTT client
        {host, BrokerIP},
        {port, BrokerPort},
        {clientid, ClientName},
        {clean_start, CleanStart},
        {msg_handler, #{publish => fun handle_publish/1, 
                        connected => fun handle_connect/1,
                        disconnected => fun handle_disconnect/1}}
    ],

    % Start the client process
    case Protocol of
        ?MQTT_V5_PROTOCOL ->
            FullPropList = PropList ++ [{proto_ver, v5}],
            emqtt:start_link(FullPropList);
        ?MQTT_V311_PROTOCOL ->
            FullPropList = PropList ++ [{proto_ver, v3}],
            emqtt:start_link(FullPropList)
    end.

% Helper function to handle clean starts and reconnects
do_connect(CleanStart, State = #{client_name := ClientName, connected := Connected}) ->
    % Connect to the MQTT broker if not already connected
    case Connected of 
        false ->
            % For now, we assume the properties are accepted as requested
            {ok, NewClientPid} = start_client_link(ClientName, CleanStart),
            {ok, Properties} = emqtt:connect(NewClientPid),
            io:format("Connected with ~p~n", [Properties]),
            {reply, ok, State#{client_pid := NewClientPid, connected := true, first_start := false}};
        true ->
            % Already connected, don't do anything
            {reply, ok, State}
    end.
