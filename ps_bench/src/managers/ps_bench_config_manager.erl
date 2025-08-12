-module(ps_bench_config_manager).

-include("ps_bench_config.hrl").

% Loading exports
-export([load_config_from_env_vars/0]).

% Retrieval exports
-export([fetch_node_name/0, fetch_selected_scenario/0, fetch_deployment_name/0, fetch_node_list/0,
    fetch_protocol_type/0, fetch_client_interface_information/0, fetch_devices_for_this_node/0,
    fetch_device_publication_frequency/1, fetch_device_payload_info/1, fetch_device_disconnect_info/1, 
    fetch_device_reconnect_info/1]).

% MQTT exports
-export([fetch_mqtt_broker_information/0, fetch_mqtt_default_qos/0, fetch_mqtt_qos_for_device/1]).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Loading Bootstrapping Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
load_config_from_env_vars() ->
    case ensure_env_vars_set() of
        ok ->
            {ok, DeviceDefDir} = fetch_device_definitions_dir(),
            {ok, DeploymentDefDir} = fetch_deployment_definitions_dir(),
            {ok, ScenarioDefDir} = fetch_scenario_definitions_dir(),
            load_config(DeviceDefDir, DeploymentDefDir, ScenarioDefDir);
        error ->
            {error, env_vars_not_set}
    end.

ensure_env_vars_set() ->
    % Validate needed keys are here
    EnvVars = application:get_all_env(),
    MissingKeys = ?ENV_REQ_KEY_LIST -- proplists:get_keys(EnvVars),

    % Missing keys is fatal
    case MissingKeys of
        [] ->
            ok;
        _ ->
            io:format("Required environment variable(s) ~p not defined.~n", [MissingKeys]),
            error
    end.


load_config(DeviceDefDir, DeploymentDefDir, ScenarioDefDir) ->

    % Load devices
    case load_device_definitions(DeviceDefDir) of
        ok ->

            % Load deployments
            case load_deployment_definitions(DeploymentDefDir) of
                ok ->

                    % Load scenario
                    case load_scenario_definitions(ScenarioDefDir) of
                        ok ->
                            ok;
                        error ->
                            {error, test_config_load_failed}
                    end;
                error ->
                    {error, test_config_load_failed}
            end;
        error ->
            {error, device_load_failed}
    end.


load_definition(FilePath, ProcessFunction) ->
    
    io:format("\tLoading ~s...~n", [filename:basename(FilePath)]),
    
    case file:consult(FilePath) of
        {ok, [Def]} ->
            case ProcessFunction(Def) of
                ok ->
                    io:format("\t> Success!~n", []),
                    ok;
                {error, Reason} ->
                    io:format("\t> Failed: ~s~n", [Reason]),
                    error
            end;

        {error, {Line, _Mod, Term}} ->
            io:format("\t> Failed: Malformed on line ~p: ~p~n", [Line, Term]),
            error;

        {error, enoent} ->
            io:format("\t> Failed: File not found at ~p~n", [FilePath]),
            error;

        {error, Error} ->
            io:format("\t> Failed: Could not read file ~p. Error: ~p~n", [FilePath, Error]),
            error
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Device Definition Processing
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

load_device_definitions(DirPath) ->

    case filelib:is_dir(DirPath) of
        true ->
            io:format("Loading device definitions from ~p...~n", [DirPath]),
            DeviceDefs = filelib:wildcard(?DEVICE_FILE_EXT, DirPath),
            FullDevicePaths =  lists:map(fun(DefFile) -> string:join([DirPath, DefFile], "/") end, DeviceDefs),
            lists:foreach(fun(X) -> load_definition(X, fun process_device_definition/1) end, FullDevicePaths),
            io:format("Done loading device definitions.~n"),
            ok;
        false ->
            io:format("Provided device definitions path was not found or is not a directory: ~p~n", [DirPath]),
            error
    end.

process_device_definition(Def) ->

    % Validate needed keys are here
    PropListKeys = proplists:get_keys(Def),
    ExtraKeys = PropListKeys -- ?DEVICE_KEY_LIST,
    MissingKeys = ?DEVICE_KEY_LIST -- PropListKeys,
    
    % Having extra keys is just a warning, disregard return of the case
    case ExtraKeys of
        [] -> ok;
        _ -> io:format("\tWarning: Unknown key(s) ~p found in definition. Ignoring...~n", [ExtraKeys])
    end,

    % Missing keys is fatal
    case MissingKeys of
        [] ->
            % Store proplist in persistent_term since it won't be edited 
            Type = proplists:get_value(?DEVICE_TYPE_PROP, Def),
            persistent_term:put({?MODULE, Type}, Def);
        _ ->
            Reason = io_lib:format("Missings key(s) ~p in definition.", [MissingKeys]),
            {error, Reason}
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Deployment Definition Processing
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

load_deployment_definitions(DirPath) ->

    case filelib:is_dir(DirPath) of
        true ->
            io:format("Loading deployment definitions from ~p...~n", [DirPath]),
            DeploymentDefs = filelib:wildcard(?DEPLOYMENT_FILE_EXT, DirPath),
            FullPaths =  lists:map(fun(DefFile) -> string:join([DirPath, DefFile], "/") end, DeploymentDefs),
            lists:foreach(fun(X) -> load_definition(X, fun process_deployment_definition/1) end, FullPaths),
            io:format("Done loading deployment definitions.~n"),
            ok;
        false ->
            io:format("Provided deployment definitions path was not found or is not a directory: ~p~n", [DirPath]),
            error
    end.

process_deployment_definition(Def) ->

    % First process top level properties
    % Validate needed keys are here
    PropListKeys = proplists:get_keys(Def),
    ExtraKeys = PropListKeys -- ?DEPLOYMENT_REQ_KEYS,
    MissingKeys = ?DEPLOYMENT_REQ_KEYS -- PropListKeys,
    
    % Having extra keys is just a warning, disregard return of the case
    case ExtraKeys of
        [] -> ok;
        _ -> io:format("\tWarning: Unknown key(s) ~p found in definition. Ignoring...~n", [ExtraKeys])
    end,

    % Missing keys is fatal
    case MissingKeys of
        [] ->
            % Now process the node section
            DeploymentName = proplists:get_value(?DEPLOYMENT_NAME_PROP, Def),
            NodeDefs = proplists:get_value(?DEPLOYMENT_NODES_PROP, Def),
            process_deployment_nodes(NodeDefs, DeploymentName);
        _ ->
            Reason = io_lib:format("Missings key(s) ~p in definition.~n", [MissingKeys]),
            {error, Reason}
    end.

process_deployment_nodes(Def, DeploymentName) ->

    % No required keys, but we do need at least one node
    case length(Def) of 
        Length when Length > 0 ->

            % Store list of nodes for the deployment
            NodeNames = proplists:get_keys(Def),
            persistent_term:put({?MODULE, DeploymentName}, NodeNames),

            % Process nodes one at a time 
            lists:foreach(fun({NodeName, Props}) -> process_deployment_node(Props, NodeName, DeploymentName) end, Def),
            ok;
        _ ->
            Reason = io_lib:format("No node definitions present in deployment.~n"),
            {error, Reason}
    end.

process_deployment_node(Def, NodeName, DeploymentName) ->

    % Validate needed keys are here
    PropListKeys = proplists:get_keys(Def),
    ExtraKeys = PropListKeys -- ?DEPLOYMENT_REQ_NODE_KEYS,
    MissingKeys = ?DEPLOYMENT_REQ_NODE_KEYS -- PropListKeys,
    
    % Having extra keys is just a warning, disregard return of the case
    case ExtraKeys of
        [] -> ok;
        _ -> io:format("\tWarning: Unknown key(s) ~p found in node ~p definition. Ignoring...~n", [ExtraKeys, NodeName])
    end,

    % Missing keys is fatal
    case MissingKeys of
        [] ->
            % Now process the node section
            DevicesInDeployment = proplists:get_value(?DEPLOYMENT_DEVICES_PROP, Def),
            persistent_term:put({?MODULE, DeploymentName, NodeName}, DevicesInDeployment);
        _ ->
            Reason = io_lib:format("Missings key(s) ~p in node ~p definition.~n", [MissingKeys, NodeName]),
            {error, Reason}
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Scenario Definition Processing
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

load_scenario_definitions(DirPath) ->

    case filelib:is_dir(DirPath) of
        true ->
            io:format("Loading scenario definitions from ~p...~n", [DirPath]),
            ScenarioDefs = filelib:wildcard(?SCENARIO_FILE_EXT, DirPath),
            FullPaths =  lists:map(fun(DefFile) -> string:join([DirPath, DefFile], "/") end, ScenarioDefs),
            lists:foreach(fun(X) -> load_definition(X, fun process_scenario_definition/1) end, FullPaths),
            io:format("Done loading scenario definitions.~n"),
            ok;
        false ->
            io:format("Provided scenario definitions path was not found or is not a directory: ~p~n", [DirPath]),
            error
    end.

process_scenario_definition(Def) ->

    % First process top level properties
    % Validate needed keys are here
    PropListKeys = proplists:get_keys(Def),
    NonReqKeys = PropListKeys -- ?SCENARIO_REQ_KEY_LIST,
    ExtraKeys =  NonReqKeys -- [?SCENARIO_INTERFACE_NAME_PROP, ?SCENARIO_INTERFACE_TYPE_PROP],
    MissingKeys = ?SCENARIO_REQ_KEY_LIST -- PropListKeys,
    
    % Having extra keys is just a warning, disregard return of the case
    case ExtraKeys of
        [] -> ok;
        _ -> io:format("\tWarning: Unknown key(s) ~p found in definition. Ignoring...~n", [ExtraKeys])
    end,

    % Missing keys is fatal
    case MissingKeys of
        [] ->
            % Store all properties except for the specific protocol settings
            ScenarioName = proplists:get_value(?SCENARIO_NAME_PROP, Def),
            PropsToSave = lists:filter(fun({Key, _Value}) -> not lists:member(Key, [?SCENARIO_PROTOCOL_CONFIG_PROP]) end, Def),
            persistent_term:put({?MODULE, ScenarioName}, PropsToSave),

            % Now process the protocol configurations to ensure they're valid
            ProtocolType = proplists:get_value(?SCENARIO_PROTOCOL_PROP, Def),
            ProtocolProps = proplists:get_value(?SCENARIO_PROTOCOL_CONFIG_PROP, Def),

            process_scenario_protocol_config(ProtocolProps, ProtocolType, ScenarioName);
        _ ->
            Reason = io_lib:format("Missings key(s) ~p in definition.~n", [MissingKeys]),
            {error, Reason}
    end.

process_scenario_protocol_config(ProtocolProps, ProtocolName, ScenarioName) ->

    % Make sure protocol is supported
    case lists:member(ProtocolName, ?SUPPORTED_PROTOCOLS) of
    true ->
        % Process properties in protocol section based on protocol
        case ProtocolName of 
            ?MQTT_V5_PROTOCOL ->
                process_mqtt_config(ProtocolProps, ProtocolName, ScenarioName);
            ?MQTT_V311_PROTOCOL ->
                process_mqtt_config(ProtocolProps, ProtocolName, ScenarioName);
            ?DDS_PROTOCOL ->
                process_dds_config(ProtocolProps, ScenarioName)
        end;
    false ->
        Reason = io_lib:format("Unsupported protocol ~p requested.", [ProtocolName]),
        {error, Reason}
end.

process_mqtt_config(ProtocolProps, ProtocolVersion, ScenarioName) ->
    % Validate needed keys are here
    PropListKeys = proplists:get_keys(ProtocolProps),
    ExtraKeys = PropListKeys -- ?MQTT_REQ_KEY_LIST,
    MissingKeys = ?MQTT_REQ_KEY_LIST -- PropListKeys,

    % Having extra keys is just a warning, disregard return of the case
    case ExtraKeys of
        [] -> ok;
        _ -> io:format("\tWarning: Unknown key(s) ~p found in MQTT properties. Ignoring...~n", [ExtraKeys])
    end,

    % Missing keys is fatal
    case MissingKeys of
        [] ->
            % Store proplist in persistent_term since it won't be edited 
            persistent_term:put({?MODULE, ScenarioName, ProtocolVersion}, ProtocolProps);
        _ ->
            Reason = io_lib:format("Missings key(s) ~p in MQTT properties.", [MissingKeys]),
            {error, Reason}
    end.

process_dds_config(_ProtocolProps, _ScenarioName) ->
    Reason = io_lib:format("DDS not currently supported.", []),
    {error, Reason}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Retrival functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
fetch_node_name() ->
    application:get_env(?ENV_NODE_NAME).

fetch_device_definitions_dir() ->
    application:get_env(?ENV_DEVICE_DEF_DIR).

fetch_deployment_definitions_dir() ->
    application:get_env(?ENV_DEPLOYMENT_DEF_DIR).

fetch_scenario_definitions_dir() ->
    application:get_env(?ENV_SCENARIO_DEF_DIR).

fetch_selected_scenario() ->
    application:get_env(?ENV_SELECTED_SCENARIO).

fetch_protocol_type() ->
    {ok, ScenarioName} = fetch_selected_scenario(),
    fetch_property_for_scenario(ScenarioName, ?SCENARIO_PROTOCOL_PROP).

fetch_client_interface_information() ->
    % This may not exist so we specify a default value
    {ok, ScenarioName} = fetch_selected_scenario(),
    {ok, InterfaceType} = fetch_property_for_scenario(ScenarioName, ?SCENARIO_INTERFACE_TYPE_PROP, ?DEFAULT_INTERFACE_TYPE),
    {ok, InterfaceName} = fetch_property_for_scenario(ScenarioName, ?SCENARIO_INTERFACE_NAME_PROP, ?DEFAULT_INTERFACE_NAME),
    {ok, InterfaceType, InterfaceName}.

fetch_mqtt_broker_information() ->
    {ok, BrokerIP} = fetch_protocol_property(?MQTT_BROKER_IP_PROP),
    {ok, BrokerPort} = fetch_protocol_property(?MQTT_BROKER_PORT_PROP),
    {ok, BrokerIP, BrokerPort}.

fetch_mqtt_default_qos() ->
    {ok, QoSList} = fetch_protocol_property(?MQTT_QOS_PROP),
    Value = proplists:get_value(?MQTT_DEFAULT_QOS_PROP, QoSList, 0),
    {ok, Value}.

fetch_mqtt_qos_for_device(DeviceType) ->
    {ok, DefaultQoS} = fetch_mqtt_default_qos(),
    {ok, QoSList} = fetch_protocol_property(?MQTT_QOS_PROP),
    Value = proplists:get_value(DeviceType, QoSList, DefaultQoS),
    {ok, Value}.

fetch_deployment_name() ->
    {ok, ScenarioName} = fetch_selected_scenario(),
    fetch_property_for_scenario(ScenarioName, ?SCENARIO_DEPLOYMENT_NAME_PROP).

fetch_node_list() ->
    {ok, DeploymentName} = fetch_deployment_name(),
   
    % Node list is saved directly under the deployment name, no properties needed
    NodeList = persistent_term:get({?MODULE, DeploymentName}, unknown_deployment),
    case NodeList of 
        unknown_deployment -> 
            {error, unknown_deployment};
        _ -> 
            {ok, NodeList}
    end.

fetch_devices_for_this_node() ->
    {ok, NodeName} = fetch_node_name(),
    fetch_devices_for_node(NodeName).

fetch_devices_for_node(NodeName) ->
    {ok, DeploymentName} = fetch_deployment_name(),
   
    % Device list is saved directly under the deployment name, no properties needed
    DeviceList = persistent_term:get({?MODULE, DeploymentName, NodeName}, unknown_deployment_node),
    case DeviceList of 
        unknown_deployment_node -> 
            {error, unknown_deployment_node};
        _ -> 
            {ok, DeviceList}
    end.

fetch_device_publication_frequency(DeviceType) ->
    fetch_property_for_device(DeviceType, ?DEVICE_PUB_FREQ_PROP).

fetch_device_payload_info(DeviceType) ->
    {ok, PayloadBytesMean} = fetch_property_for_device(DeviceType, ?DEVICE_SIZE_MEAN_PROP),
    {ok, PayloadBytesVariance} = fetch_property_for_device(DeviceType, ?DEVICE_SIZE_VARIANCE_PROP),
    {ok, PayloadBytesMean, PayloadBytesVariance}.

fetch_device_disconnect_info(DeviceType) ->
    {ok, PeriodMs} = fetch_property_for_device(DeviceType, ?DEVICE_DISCON_CHECK_MS_PROP),
    {ok, Pct} = fetch_property_for_device(DeviceType, ?DEVICE_DISCON_PCT_PROP),
    {ok, PeriodMs, Pct}.

fetch_device_reconnect_info(DeviceType) ->
    {ok, PeriodMs} = fetch_property_for_device(DeviceType, ?DEVICE_RECON_CHECK_MS_PROP),
    {ok, Pct} = fetch_property_for_device(DeviceType, ?DEVICE_RECON_PCT_PROP),
    {ok, PeriodMs, Pct}.


fetch_property_for_scenario(ScenarioName, PropName) ->
    fetch_property_for_scenario(ScenarioName, PropName, undefined).

fetch_property_for_scenario(ScenarioName, PropName, DefaultValue) ->
    % Retrieve prop list and make sure the scenario exists
    Def = persistent_term:get({?MODULE, ScenarioName}, unknown_scenario),

    case Def of 
        unknown_scenario -> 
            {error, unknown_scenario};
        _ -> 
            case proplists:get_value(PropName, Def, DefaultValue) of 
            undefined -> 
                {error, unknown_property};
            Value ->
                {ok, Value}
        end
    end.

fetch_protocol_property(Key) ->
    {ok, ScenarioName} = fetch_selected_scenario(),
    {ok, Protocol} = fetch_protocol_type(),
    ProtocolProps = persistent_term:get({?MODULE, ScenarioName, Protocol}),
    case proplists:get_value(Key, ProtocolProps) of
        undefined -> 
            {error, unknown_property};
        Value ->
            {ok, Value}
    end.

fetch_property_for_device(DeviceType, PropName) ->
    % Retrieve prop list and make sure the device exists
    Def = persistent_term:get({?MODULE, DeviceType}, unknown_device),

    case Def of 
        unknown_device -> 
            {error, unknown_device};
        _ -> 
            Value = proplists:get_value(PropName, Def),
            {ok, Value}
    end.