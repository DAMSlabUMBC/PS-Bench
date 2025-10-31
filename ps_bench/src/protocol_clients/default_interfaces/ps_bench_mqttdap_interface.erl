-module(ps_bench_mqttdap_interface).
-behaviour(gen_server).

-include("ps_bench_config.hrl").
%% Public API
-export([start_link/3]).

%% gen_server callbacks (standard Erlang/OTP stuff)
-export([init/1, handle_call/3,
         handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

start_link(ScenarioName, ClientName0, OwnerPid) ->
    RegName = ps_bench_utils:convert_to_atom(ClientName0),
    gen_server:start_link({local, RegName}, ?MODULE, {ScenarioName, RegName, OwnerPid}, []).

%%%===================================================================
%%% gen_server callbacks (these get called by the Erlang/OTP framework)
%%%===================================================================

%% Initialize the client
%% This sets up the initial state when a new MQTT client is created
init({ScenarioName, RegName, OwnerPid}) ->
    ClientIdBin = ps_bench_utils:convert_to_binary(RegName),
    {ok, #{scenario_name => ScenarioName,
           client_name => ClientIdBin,              % My client ID
           reg_name => RegName,
           client_pid => 0,
           owner_pid => OwnerPid,                   % Who to send events to
           connected => false,
           first_start => true,
           % MQTT-DAP specific state - these store the purposes for this client
           message_purpose => <<"">>,               % DAP-MP: What purposes I publish with
           subscription_purpose => <<"">>,          % DAP-SP: What purpose I subscribe with
           allowed_purposes => [],                  % List of purposes I'm allowed to use
           operation_requests => #{}                % Track pending GDPR operations
          }}.

%% Connect to the broker
%% These three calls handle different connection scenarios
handle_call(connect, _From, State = #{first_start := FirstStart}) ->
    do_connect(FirstStart, State);

handle_call(connect_clean, _From, State) ->
    % Force a clean connection (forget old session)
    do_connect(false, State);

handle_call(reconnect, _From, State) ->
    % Reconnect after disconnect
    do_connect(true, State);

%% Subscribe to topics with MQTT-DAP purpose filter
%% This tells the broker: "I want to subscribe, and my purpose is SP"
%% The broker should only deliver messages where SP is compatible with the message's MP
handle_call({subscribe, Properties, Topics}, _From,
            State = #{client_pid := ClientPid, connected := Connected, subscription_purpose := SP}) when Connected == true ->
    io:format("~n[MQTT-DAP] mqttdap_interface subscribe called. ClientPid=~p Connected=~p SP=~p Topics=~p~n",
              [ClientPid, Connected, SP, Topics]),
    % Add DAP-SP property to subscription if configured (as User-Property)
    DAPProperties = case SP of
        <<"">> -> Properties;  % No purpose set, just use regular properties
        _ -> Properties#{'User-Property' => [{<<"DAP-SP">>, SP}]}  % Add subscription purpose
    end,
    io:format("~n[MQTT-DAP] Calling emqtt:subscribe with DAPProperties=~p~n", [DAPProperties]),
    Result = emqtt:subscribe(ClientPid, DAPProperties, Topics),
    io:format("~n[MQTT-DAP] emqtt:subscribe returned: ~p~n", [Result]),
    {reply, ok, State};

handle_call({subscribe, _Properties, _Topics}, _From, State = #{connected := Connected}) when Connected == false ->
    % Not connected, ignore subscription request
    io:format("~n[MQTT-DAP] Subscribe called but not connected, ignoring~n"),
    {reply, ok, State};

handle_call({subscribe, Properties, Topics}, _From, State) ->
    % Catch-all for debugging - this shouldn't normally be reached
    io:format("~n[MQTT-DAP] Subscribe catch-all reached! Properties=~p Topics=~p State=~p~n",
              [Properties, Topics, State]),
    {reply, ok, State};

%% Publish a message with MQTT-DAP properties
%% THIS IS THE KEY FUNCTION! This is where we add purpose-based access control
handle_call({publish, Properties, Topic, Payload, PubOpts},
            _From,
            State = #{client_pid := ClientPid, connected := Connected,
                     message_purpose := MP, client_name := ClientName})
  when is_binary(Topic), is_binary(Payload) ->
    case Connected of
        true ->
            % Add timestamp to payload (PS-Bench requirement for tracking)
            TimeNs = erlang:system_time(nanosecond),
            Payload1 = <<TimeNs:64/unsigned, Payload/binary>>,

            % Build MQTT-DAP user properties list
            % In MQTT v5, custom properties must be in 'User-Property' list
            UserProps = [
                {<<"DAP-Allow">>, <<"1">>},        % Required: Explicit consent to process data
                {<<"DAP-ClientID">>, ClientName}   % Required: Who is publishing (for tracking)
            ],

            % Add message purpose (MP) if configured
            % MP says: "This data can ONLY be used for these purposes"
            UserPropsWithMP = case MP of
                <<"">> ->
                    io:format("~n[MQTT-DAP] PUBLISH Client=~p Topic=~p MP=EMPTY~n", [ClientName, Topic]),
                    UserProps;  % No purpose filtering
                _ ->
                    io:format("~n[MQTT-DAP] PUBLISH Client=~p Topic=~p MP=~p~n", [ClientName, Topic, MP]),
                    UserProps ++ [{<<"DAP-MP">>, MP}]  % Add purpose filter
            end,

            % Merge user properties with any existing properties
            FinalProperties = Properties#{'User-Property' => UserPropsWithMP},

            % Actually publish the message with all the MQTT-DAP properties
            emqtt:publish(ClientPid, Topic, FinalProperties, Payload1, PubOpts),
            {reply, {ok, published}, State};
        false ->
            % Not connected, can't publish
            {reply, {ok, not_connected}, State}
    end;

%% API: Set what purpose I want to publish with (MP)
%% Example: gen_server:call(ClientPid, {set_message_purpose, "production-metrics/output"})
handle_call({set_message_purpose, PurposeFilter}, _From, State = #{client_name := ClientName}) ->
    MPBin = ps_bench_utils:convert_to_binary(PurposeFilter),
    io:format("~n[MQTT-DAP] set_message_purpose: Client=~p MP=~p~n", [ClientName, MPBin]),
    {reply, ok, State#{message_purpose := MPBin}};

%% API: Set what purpose I want to subscribe with (SP)
%% Example: gen_server:call(ClientPid, {set_subscription_purpose, "analytics"})
handle_call({set_subscription_purpose, PurposeFilter}, _From, State) ->
    {reply, ok, State#{subscription_purpose := ps_bench_utils:convert_to_binary(PurposeFilter)}};

%% MQTT-DAP: Register an operation (C1 - registration operations)
%% This is for things like registering what purposes I want to publish/subscribe with
%% Example: Register that I want to subscribe for "analytics" purpose
handle_call({register_operation, OperationName, OperationData}, _From,
            State = #{client_pid := ClientPid, connected := Connected}) when Connected == true ->
    % Build the registration topic (e.g., "dap/reg/SP/FetchContact")
    RegTopic = construct_registration_topic(OperationName),

    % Add operation metadata as User-Property
    Properties = #{
        'User-Property' => [
            {<<"DAP-Operation">>, ps_bench_utils:convert_to_binary(OperationName)},
            {<<"DAP-ClientID">>, maps:get(client_name, State)}
        ]
    },

    % Publish the registration request
    emqtt:publish(ClientPid, RegTopic, Properties, OperationData, [{qos, 1}]),
    {reply, ok, State};

%% MQTT-DAP: Invoke an operation (C2/C3 - GDPR operations)
%% This is for GDPR rights: access my data, delete my data, rectify my data, etc.
%% Example: "Delete all my messages from the past month"
handle_call({invoke_operation, OperationName, OpInfo, Payload}, _From,
            State = #{client_pid := ClientPid, connected := Connected}) when Connected == true ->
    % Generate unique correlation ID so we can match the response to this request
    CorrData = generate_correlation_data(),
    ResponseTopic = <<"dap/response/", (maps:get(client_name, State))/binary>>,

    % Subscribe to my personal response topic (so broker can send results back to me)
    _ = emqtt:subscribe(ClientPid, #{}, [{ResponseTopic, [{qos, 1}]}]),

    % Build the operation request with all required MQTT-DAP properties as User-Property
    OpTopic = <<"dap/operation/request">>,
    Properties = #{
        'User-Property' => [
            {<<"DAP-Operation">>, ps_bench_utils:convert_to_binary(OperationName)},
            {<<"DAP-ClientID">>, maps:get(client_name, State)},
            {<<"DAP-OpInfo">>, ps_bench_utils:convert_to_binary(OpInfo)}
        ],
        'Response-Topic' => ResponseTopic,
        'Correlation-Data' => CorrData
    },

    % Send the operation request to the broker
    emqtt:publish(ClientPid, OpTopic, Properties, Payload, [{qos, 1}]),

    % Remember this operation so we can track when we get a response
    OpRequests = maps:get(operation_requests, State),
    NewOpRequests = OpRequests#{CorrData => #{operation => OperationName,
                                               timestamp => erlang:system_time(nanosecond),
                                               status => pending}},

    {reply, {ok, CorrData}, State#{operation_requests := NewOpRequests}};

%% Unsubscribe from topics
handle_call({unsubscribe, Properties, Topics}, _From, State = #{client_pid := ClientPid}) ->
    {ok, _Props, _ReasonCodes} = emqtt:unsubscribe(ClientPid, Properties, Topics),
    {reply, ok, State};

%% Disconnect from the broker
handle_call(disconnect, _From, State = #{client_pid := ClientPid, connected := Connected}) ->
    case Connected of
        true ->
            ok = emqtt:disconnect(ClientPid),
            {reply, ok, State#{connected := false}};
        false ->
            % Already disconnected, nothing to do
            {reply, ok, State}
    end;

%% Stop the client (called during shutdown)
handle_call(stop, _From, State) ->
    {reply, ok, State};

%% Catch-all for any other calls we don't handle
handle_call(Message, From, State) ->
    io:format("~n[MQTT-DAP] CATCH-ALL handler reached! Message=~p From=~p State=~p~n",
              [Message, From, State]),
    {reply, ok, State}.

%% Cast messages (asynchronous calls) - we don't use these
handle_cast(_, State) ->
    {noreply, State}.

%% Handle info messages (from other processes)
%% This catches things like EXIT signals when something crashes
handle_info(Info, State) ->
    case Info of
        {'EXIT', _Pid, Reason} ->
            % Something crashed, handle the exception
            handle_exception(Reason, State);
        _ ->
            % Unknown message, just log it
            ps_bench_utils:log_message("Received unknown info: ~p", [Info]),
            {noreply, State}
    end.

%% Handle exceptions and crashes
handle_exception(Reason, State) ->
    case Reason of
        {shutdown, econnrefused} ->
            % Broker refused our connection - this is fatal
            ps_bench_utils:log_message("ERROR: MQTT Broker refused connection"),
            {kill, Reason};
        normal ->
            % Normal shutdown, nothing to worry about
            {noreply, State};
        _ ->
            % Some other error, log it but keep going
            ps_bench_utils:log_message("ERROR: Received termination signal: ~p", [Reason]),
            {noreply, State}
    end.

%% Clean up when shutting down
terminate(_Reason, _State) ->
    ok.

%% Handle hot code reloading (Erlang feature)
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal helper functions
%%% These are the behind-the-scenes functions that do the actual work
%%%===================================================================

%% Start an MQTT client connection
%% This sets up all the connection parameters and callbacks
start_client_link(ClientName, CleanStart, OwnerPid) ->
    % Get broker connection info from config
    {ok, BrokerIP, BrokerPort} = ps_bench_config_manager:fetch_mqtt_broker_information(),
    {ok, Protocol} = ps_bench_config_manager:fetch_protocol_type(),

    % Build connection properties
    PropList = [
        {host, BrokerIP},
        {port, BrokerPort},
        {clientid, ClientName},
        {clean_start, CleanStart},  % Clean session or resume existing?
        % Set up callbacks for when things happen:
        {msg_handler, #{disconnected => fun(Reason) -> disconnect_event(OwnerPid, Reason, ClientName) end,
                        publish => fun(Msg) -> publish_event(OwnerPid, Msg, ClientName) end}}
    ],

    % Trap EXIT signals so we can handle crashes gracefully
    process_flag(trap_exit, true),

    % IMPORTANT: MQTT-DAP REQUIRES MQTT v5!
    % User properties (DAP-MP, DAP-SP, etc.) are only in MQTT v5
    case Protocol of
        ?MQTT_V5_PROTOCOL ->
            FullPropList = PropList ++ [{proto_ver, v5}],
            ok = ensure_emqtt_started(),
            emqtt:start_link(FullPropList);
        ?MQTT_V311_PROTOCOL ->
            ps_bench_utils:log_message("ERROR: MQTT-DAP requires MQTT v5"),
            {error, mqtt_v5_required}
    end.

%% Actually perform the connection to the broker
%% This is called by connect, connect_clean, and reconnect handlers
do_connect(CleanStart, State = #{client_name := ClientName, owner_pid := OwnerPid, connected := Connected}) ->
    case Connected of
        false ->
            % Not connected, so let's connect
            case start_client_link(ClientName, CleanStart, OwnerPid) of
                {ok, NewClientPid} ->
                    % Client started, now actually connect to broker
                    case emqtt:connect(NewClientPid) of
                        {ok, Properties} ->
                            % Success! Notify owner and update state
                            connect_event(OwnerPid, Properties, ClientName),
                            {reply, {ok, new_connection},
                             State#{client_pid := NewClientPid, connected := true, first_start := false}};
                        {error, Reason} ->
                            % Connection failed
                            ps_bench_utils:log_message("MQTT connect failed for ~s with reason ~p", [ClientName, Reason]),
                            {reply, {error, Reason}, State}
                    end;
                {error, Reason} ->
                    % Couldn't even start the client
                    ps_bench_utils:log_message("MQTT client start_link failed (~p): ~p", [ClientName, Reason]),
                    {reply, {error, Reason}, State};
                Res ->
                    % Some other weird error
                    ps_bench_utils:log_message("Error ~p", [Res]),
                    {reply, {error, Res}, State}
            end;
        true ->
            % Already connected, nothing to do
            {reply, {ok, already_connected}, State}
    end.

%% Send "connected" event to the owner process
%% This lets PS-Bench know we successfully connected
connect_event(OwnerPid, _Properties, ClientName) ->
    TimeNs = erlang:system_time(nanosecond),
    OwnerPid ! {?CONNECTED_MSG, {TimeNs}, ClientName},
    ok.

%% Send "disconnected" event to the owner process
%% This lets PS-Bench know we got disconnected (intentional or not)
disconnect_event(OwnerPid, Reason, ClientName) ->
    TimeNs = erlang:system_time(nanosecond),
    OwnerPid ! {?DISCONNECTED_MSG, {TimeNs, Reason}, ClientName},
    ok.

%% Send "received message" event to the owner process
%% THIS IS IMPORTANT: We extract MQTT-DAP properties and forward them with the message!
%% The metrics plugins need these properties to check if PBAC is working correctly
publish_event(OwnerPid, Msg = #{topic := Topic, payload := Payload, properties := Properties}, ClientName) ->
    TimeNs = erlang:system_time(nanosecond),

    % Extract all the MQTT-DAP properties from the message
    % This includes DAP-MP, DAP-SP, DAP-ClientID, etc.
    DAPProps = extract_dap_properties(Properties),

    % Forward the message with MQTT-DAP metadata to PS-Bench
    % The PBAC correctness plugin will use these properties to check filtering
    OwnerPid ! {?PUBLISH_RECV_MSG, {TimeNs, Topic, Payload, DAPProps}, ClientName},
    ok;

publish_event(OwnerPid, #{topic := Topic, payload := Payload}, ClientName) ->
    % Fallback for messages that don't have properties (shouldn't happen with MQTT v5)
    TimeNs = erlang:system_time(nanosecond),
    OwnerPid ! {?PUBLISH_RECV_MSG, {TimeNs, Topic, Payload, #{}}, ClientName},
    ok.

%% Extract only the MQTT-DAP specific properties from a message
%% MQTT-DAP properties come in the 'User-Property' list
%% This makes it easier for metrics plugins to check PBAC correctness
extract_dap_properties(Properties) when is_map(Properties) ->
    % Check if there are User-Property entries
    case maps:get('User-Property', Properties, []) of
        [] ->
            #{};
        UserProps when is_list(UserProps) ->
            % DAP property names we care about (as binaries)
            DAPKeys = [<<"DAP-Allow">>, <<"DAP-ClientID">>, <<"DAP-MP">>, <<"DAP-SP">>,
                       <<"DAP-Operation">>, <<"DAP-OpInfo">>, <<"DAP-Status">>,
                       <<"DAP-Reason">>, <<"DAP-Deadline">>, <<"DAP-Retroactive">>],

            % Filter User-Property list to only DAP-* properties
            % Convert to map for easier access
            lists:foldl(fun({Key, Value}, Acc) ->
                case lists:member(Key, DAPKeys) of
                    true ->
                        % Keep as binary key for consistency
                        Acc#{Key => Value};
                    false ->
                        Acc
                end
            end, #{}, UserProps);
        _ ->
            #{}
    end;

extract_dap_properties(_) ->
    % Properties weren't a map? Return empty map
    #{}.

%% Build registration topic for an operation
%% Example: "FetchContact" becomes "dap/reg/SP/FetchContact"
construct_registration_topic(OperationName) ->
    OpNameBin = ps_bench_utils:convert_to_binary(OperationName),
    <<"dap/reg/SP/", OpNameBin/binary>>.

%% Generate a unique ID for tracking operation requests/responses
%% Combines timestamp + random number to ensure uniqueness
generate_correlation_data() ->
    Timestamp = erlang:system_time(nanosecond),
    Random = rand:uniform(1000000),
    list_to_binary(io_lib:format("~p_~p", [Timestamp, Random])).

%% Make sure the emqtt application is started before we use it
ensure_emqtt_started() ->
    case application:ensure_all_started(emqtt) of
        {ok, _} -> ok;
        {error, {emqtt, {already_started, _}}} -> ok;  % Already started, that's fine
        {error, Reason} -> exit({emqtt_not_started, Reason})  % Can't start, fatal error
    end.
