-module(ps_bench_mqtt_erlang_interface).
-behaviour(gen_server).

-include("ps_bench_config.hrl").

%% public
-export([start_link/2]).

%% gen_server callbacks
-export([init/1, handle_call/3,
         handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

start_link(ScenarioName, ClientName) ->
    gen_server:start_link({local, list_to_atom(ClientName)}, ?MODULE, [{ScenarioName, ClientName}], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([{ScenarioName, ClientName}]) ->
    {ok, #{scenario_name => ScenarioName, client_name => ClientName, client_pid => 0, connected => false, first_start => true}}.

handle_call({connect, MsgHandlers}, _From, State = #{first_start := FirstStart}) ->
    % If not specifically told, we start clean by default, then preserve old sessions on reconnects
    do_connect(FirstStart, State, MsgHandlers);

handle_call({connect_clean, MsgHandlers}, _From, State) ->
    % Force clean connect
    do_connect(false, State, MsgHandlers);

handle_call({reconnect, MsgHandlers}, _From, State) ->
    % Force restablishment of session
    % By the MQTT standard, this just starts a clean session if none existed previously
    do_connect(true, State, MsgHandlers);

handle_call({subscribe, Properties, Topics}, _From, State = #{client_pid := ClientPid}) ->
    % Subscribe with the on Topic with Options
    {ok, _Props, _ReasonCodes} = emqtt:subscribe(ClientPid, Properties, Topics),
    {reply, ok, State};

handle_call({publish, Properties, Topic, Payload, PubOpts}, _From, State = #{client_pid := ClientPid, connected := Connected}) when is_binary(Topic), is_binary(Payload) ->
    % Need to check QoS
    case Connected of
        true ->
            case lists:keysearch(qos, 1, PubOpts) of
                % QoS >= 1, we need to return PacketId
                {value, {qos, QoS}} when QoS >= 1  -> 
                    TimeNs = erlang:monotonic_time(nanosecond),
                    PayloadWithTime = <<TimeNs:64/unsigned, Payload/binary>>,
                    emqtt:publish(ClientPid, Topic, Properties, PayloadWithTime, PubOpts),
                    {reply, ok, State};
                % QoS wasn't specified or was given a value of 0
                _ ->
                    TimeNs = erlang:monotonic_time(nanosecond),
                    PayloadWithTime = <<TimeNs:64/unsigned, Payload/binary>>,
                    emqtt:publish(ClientPid, Topic, Properties, PayloadWithTime, PubOpts),
                    {reply, ok, State}
            end;
        false ->
            {reply, ok, State}
    end;
    

handle_call({unsubscribe, Properties, Topics}, _From, State = #{client_pid := ClientPid}) ->
    % Subscribe with the on Topic with Options
    {ok, _Props, _ReasonCodes} = emqtt:unsubscribe(ClientPid, Properties, Topics),
    {reply, ok, State};

handle_call(disconnect, _From, State = #{client_pid := ClientPid, connected := Connected}) ->
    % Disconnect from MQTT broker if connected
    case Connected of 
        true ->
            % For now, we assume the properties are accepted as requested
            ok = emqtt:disconnect(ClientPid),
            {reply, ok, State#{connected := false}};
        false ->
            % Not connected don't do anything
            {reply, ok, State}
    end;

handle_call(_, _, State) ->
    {reply, ok, State}.

handle_cast(stop, State = #{client_pid := ClientPid}) ->
    % Shutdown the client
    ok = emqtt:stop(ClientPid),
    {noreply, State};

handle_cast(_, State) ->
    {noreply, State}.
    
handle_info(_Info, State) ->
    %% no info logic yet, just fulfill behaviour
    {noreply, State}.

terminate(Reason, _State) -> 
    ps_bench_utils:log_message("Terminate with reason ~p",[Reason]),
    ok.

code_change(_OldVsn, State, _Extra) -> 
    {ok, State}.

start_client_link(ClientName, CleanStart, MsgHandlers) ->
    
    {ok, BrokerIP, BrokerPort} = ps_bench_config_manager:fetch_mqtt_broker_information(),
    {ok, Protocol} = ps_bench_config_manager:fetch_protocol_type(),

    % Configure properties
    PropList = [
        {owner, self()}, % This process should get notifications from the MQTT client
        {host, BrokerIP},
        {port, BrokerPort},
        {clientid, ClientName},
        {clean_start, CleanStart},
        {msg_handler, MsgHandlers}
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
do_connect(CleanStart, State = #{client_name := ClientName, connected := Connected}, MsgHandlers) ->
    % Connect to the MQTT broker if not already connected
    case Connected of 
        false ->
            % For now, we assume the properties are accepted as requested
            {ok, NewClientPid} = start_client_link(ClientName, CleanStart, MsgHandlers),
            {ok, _Properties} = emqtt:connect(NewClientPid),
            {reply, {ok, new_connection}, State#{client_pid := NewClientPid, connected := true, first_start := false}};
        true ->
            % Already connected, don't do anything
            {reply, {ok, already_connected}, State}
    end.