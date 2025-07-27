-module(ps_bench_default_erlang_client).
-behaviour(gen_server).

%% public
-export([start_link/2]).

%% gen_server callbacks
-export([init/1, handle_call/3,
         handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

start_link(TestName, ClientName) ->
    gen_server:start_link(?MODULE, [{TestName, ClientName}], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([{TestName, ClientName}]) ->
    % Fetch required config options
    %{ok, InterfaceName} = application:get_env(client_interface_name),
    %{ok, InterfaceType} = application:get_env(client_interface_type),

    {ok, #{test_name => TestName, client_name => ClientName, client_pid => 0, clean_start => true}}.

handle_call(connect, _From, State = #{client_name := ClientName, client_pid := ClientPid, clean_start := CleanStart}) when ClientPid =:= 0 ->
    % Connect to the MQTT broker
    {ok, NewClientPid} = start_client_link(ClientName, CleanStart),
    {ok, Properties} = emqtt:connect(NewClientPid),
    io:format("Connected with ~p~n", [Properties]),
    % For now, we assume the properties are accepted as requested

    {reply, ok, State#{client_pid := NewClientPid, clean_start := false}};

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

handle_call(disconnect, _From, State = #{client_pid := ClientPid}) ->
    % Subscribe with the on Topic with Options
    ok = emqtt:disconnect(ClientPid),
    {reply, ok, State#{client_pid := 0}};

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

start_client_link(ClientName, CleanStart) when is_boolean(CleanStart) ->
    
    {ok, BrokerIP} = application:get_env(broker),
    {ok, BrokerPort} = application:get_env(port),

    % Configure properties
    StartPropList = [
        {owner, self()}, % This process should get notifications from the MQTT client
        {host, BrokerIP},
        {port, BrokerPort},
        {clientid, ClientName},
        {clean_start, CleanStart},
        {proto_ver, v5}, % TODO Specify from config
        {msg_handler, #{publish => fun handle_publish/1, 
                        connected => fun handle_connect/1,
                        disconnected => fun handle_disconnect/1}}
    ],

    % Start the client process
    emqtt:start_link(StartPropList).
