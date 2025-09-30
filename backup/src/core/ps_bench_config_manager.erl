-module(ps_bench_config_manager).

-include("ps_bench_config.hrl").

% Loading exports
-export([load_config_from_env_vars/0]).

% Retrieval exports
-export([fetch_node_name/0, fetch_selected_scenario/0, fetch_scenario_duration/0, fetch_deployment_name/0, fetch_node_name_list/0,
    fetch_host_for_node_name/1, fetch_host_for_node_name/2, fetch_rng_seed_for_node/1, fetch_node_list/0, fetch_protocol_type/0, fetch_devices_for_this_node/0,
    fetch_device_publication_frequency/1, fetch_device_payload_info/1, fetch_device_disconnect_info/1, 
    fetch_device_reconnect_info/1]).

% MQTT exports
-export([fetch_mqtt_client_module/0, fetch_mqtt_broker_information/0, fetch_mqtt_default_qos/0, fetch_mqtt_qos_for_device/1]).

% DDS exports
-export([fetch_dds_nif_information/0, fetch_dds_domain_id/0, fetch_dds_config_file_path/0, fetch_dds_qos_profile/0]).

% Metric exports
-export([fetch_metrics_output_dir/0, fetch_python_metric_engine_path/0,
    fetch_python_metric_plugins/0, fetch_erlang_metric_plugins/0, using_hw_poll/0, fetch_metric_hw_poll_period/0]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Loading Bootstrapping Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
load_config_from_env_vars() ->
    case ensure_env_vars_set() of
        ok ->
            % Store the application name holding the env vars so we can fetch later
            {ok, ApplicationName} = application:get_application(),
            persistent_term:put({?MODULE, app_name}, ApplicationName),

            ps_bench_utils:log_state_change("Loading Config"),
            
            % Load config
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
            ps_bench_utils:log_message("Required environment variable(s) ~p not defined.", [MissingKeys]),
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
    
    ps_bench_utils:log_message("\tLoading ~s...", [filename:basename(FilePath)]),
    
    case file:consult(FilePath) of
        {ok, [Def]} ->
            case ProcessFunction(Def) of
                ok ->
                    ps_bench_utils:log_message("\t> Success!", []),
                    ok;
                {error, Reason} ->
                    ps_bench_utils:log_message("\t> Failed: ~s", [Reason]),
                    error
            end;

        {error, {Line, _Mod, Term}} ->
            ps_bench_utils:log_message("\t> Failed: Malformed on line ~p: ~p", [Line, Term]),
            error;

        {error, enoent} ->
            ps_bench_utils:log_message("\t> Failed: File not found at ~p", [FilePath]),
            error;

        {error, Error} ->
            ps_bench_utils:log_message("\t> Failed: Could not read file ~p. Error: ~p", [FilePath, Error]),
            error
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Device Definition Processing
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

load_device_definitions(DirPath) ->

    case filelib:is_dir(DirPath) of
        true ->
            ps_bench_utils:log_message("Loading device definitions from ~p...", [DirPath]),
            DeviceDefs = filelib:wildcard(?DEVICE_FILE_EXT, DirPath),
            FullDevicePaths =  lists:map(fun(DefFile) -> string:join([DirPath, DefFile], "/") end, DeviceDefs),
            lists:foreach(fun(X) -> load_definition(X, fun process_device_definition/1) end, FullDevicePaths),
            ps_bench_utils:log_message("Done loading device definitions."),
            ok;
        false ->
            ps_bench_utils:log_message("Provided device definitions path was not found or is not a directory: ~p", [DirPath]),
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
        _ -> ps_bench_utils:log_message("\tWarning: Unknown key(s) ~p found in definition. Ignoring...", [ExtraKeys])
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
            ps_bench_utils:log_message("Loading deployment definitions from ~p...", [DirPath]),
            DeploymentDefs = filelib:wildcard(?DEPLOYMENT_FILE_EXT, DirPath),
            FullPaths =  lists:map(fun(DefFile) -> string:join([DirPath, DefFile], "/") end, DeploymentDefs),
            lists:foreach(fun(X) -> load_definition(X, fun process_deployment_definition/1) end, FullPaths),
            ps_bench_utils:log_message("Done loading deployment definitions."),
            ok;
        false ->
            ps_bench_utils:log_message("Provided deployment definitions path was not found or is not a directory: ~p", [DirPath]),
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
        _ -> ps_bench_utils:log_message("\tWarning: Unknown key(s) ~p found in definition. Ignoring...", [ExtraKeys])
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
            persistent_term:put({?MODULE, DeploymentName, node_names}, NodeNames),

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
        _ -> ps_bench_utils:log_message("\tWarning: Unknown key(s) ~p found in node ~p definition. Ignoring...", [ExtraKeys, NodeName])
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
            ps_bench_utils:log_message("Loading scenario definitions from ~p...", [DirPath]),
            ScenarioDefs = filelib:wildcard(?SCENARIO_FILE_EXT, DirPath),
            FullPaths =  lists:map(fun(DefFile) -> string:join([DirPath, DefFile], "/") end, ScenarioDefs),
            lists:foreach(fun(X) -> load_definition(X, fun process_scenario_definition/1) end, FullPaths),
            ps_bench_utils:log_message("Done loading scenario definitions."),
            ok;
        false ->
            ps_bench_utils:log_message("Provided scenario definitions path was not found or is not a directory: ~p", [DirPath]),
            error
    end.

process_scenario_definition(Def) ->

    % First process top level properties
    % Validate needed keys are here
    PropListKeys = proplists:get_keys(Def),
    ExtraKeys = PropListKeys -- ?SCENARIO_REQ_KEY_LIST,
    MissingKeys = ?SCENARIO_REQ_KEY_LIST -- PropListKeys,
    
    % Having extra keys is just a warning, disregard return of the case
    case ExtraKeys of
        [] -> ok;
        _ -> ps_bench_utils:log_message("\tWarning: Unknown key(s) ~p found in definition. Ignoring...", [ExtraKeys])
    end,

    % Missing keys is fatal
    case MissingKeys of
        [] ->
            % Store all properties except for the specific protocol settings
            ScenarioName = proplists:get_value(?SCENARIO_NAME_PROP, Def),
            PropsToSave = lists:filter(fun({Key, _Value}) -> not lists:member(Key, [?SCENARIO_PROTOCOL_CONFIG_PROP, ?SCENARIO_METRIC_CONFIG_PROP]) end, Def),
            persistent_term:put({?MODULE, ScenarioName}, PropsToSave),

            % Now process the protocol configurations to ensure they're valid
            ProtocolType = proplists:get_value(?SCENARIO_PROTOCOL_PROP, Def),
            ProtocolProps = proplists:get_value(?SCENARIO_PROTOCOL_CONFIG_PROP, Def),
            process_scenario_protocol_config(ProtocolProps, ProtocolType, ScenarioName),

            % Now process the metric configurations to ensure they're valid
            MetricProps = proplists:get_value(?SCENARIO_METRIC_CONFIG_PROP, Def),
            process_scenario_metric_config(MetricProps, ScenarioName);
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
                process_dds_config(ProtocolProps, ProtocolName, ScenarioName)
        end;
    false ->
        Reason = io_lib:format("Unsupported protocol ~p requested.", [ProtocolName]),
        {error, Reason}
end.

process_mqtt_config(ProtocolProps, ProtocolVersion, ScenarioName) ->
    % Validate needed keys are here
    PropListKeys = proplists:get_keys(ProtocolProps),
    NonReqKeys = PropListKeys -- ?MQTT_REQ_KEY_LIST,
    ExtraKeys = NonReqKeys -- [?MQTT_CLIENT_INTERFACE_MODULE_PROP, ?MQTT_QOS_PROP],
    MissingKeys = ?MQTT_REQ_KEY_LIST -- PropListKeys,

    % Having extra keys is just a warning, disregard return of the case
    case ExtraKeys of
        [] -> ok;
        _ -> ps_bench_utils:log_message("\tWarning: Unknown key(s) ~p found in MQTT properties. Ignoring...", [ExtraKeys])
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

process_dds_config(ProtocolProps, ProtocolName, ScenarioName) ->
    % Validate needed keys are here
    PropListKeys = proplists:get_keys(ProtocolProps),
    NonReqKeys = PropListKeys -- ?DDS_REQ_KEY_LIST,
    ExtraKeys = NonReqKeys -- [?DDS_NIF_MODULE_PROP, ?DDS_NIF_FULL_PATH_PROP, ?DDS_QOS_PROFILE_PROP, ?DDS_CONFIG_FILE_PATH_PROP],
    MissingKeys = ?DDS_REQ_KEY_LIST -- PropListKeys,

    % Having extra keys is just a warning, disregard return of the case
    case ExtraKeys of
        [] -> ok;
        _ -> ps_bench_utils:log_message("\tWarning: Unknown key(s) ~p found in DDS properties. Ignoring...", [ExtraKeys])
    end,

    % Missing keys is fatal
    case MissingKeys of
        [] ->
            % Store proplist in persistent_term since it won't be edited 
            persistent_term:put({?MODULE, ScenarioName, ProtocolName}, ProtocolProps);
        _ ->
            Reason = io_lib:format("Missings key(s) ~p in DDS properties.", [MissingKeys]),
            {error, Reason}
    end.

process_scenario_metric_config(MetricProps, ScenarioName) ->
    % Validate needed keys are here
    PropListKeys = proplists:get_keys(MetricProps),
    NonReqKeys = PropListKeys -- ?METRIC_REQ_KEY_LIST,
    ExtraKeys =  NonReqKeys -- [?METRIC_PYTHON_ENGINE_PATH, ?METRIC_RESULTS_DIR_PROP, ?METRIC_HW_STATS_POLL_PERIOD],
    MissingKeys = ?METRIC_REQ_KEY_LIST -- PropListKeys,

    % Having extra keys is just a warning, disregard return of the case
    case ExtraKeys of
        [] -> ok;
        _ -> ps_bench_utils:log_message("\tWarning: Unknown key(s) ~p found in metric properties. Ignoring...", [ExtraKeys])
    end,

    % Missing keys is fatal
    case MissingKeys of
        [] ->
            % Store proplist in persistent_term since it won't be edited 
            persistent_term:put({?MODULE, ScenarioName, ?METRIC_STORAGE_CONSTANT}, MetricProps);
        _ ->
            Reason = io_lib:format("Missings key(s) ~p in metric properties.", [MissingKeys]),
            {error, Reason}
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Retrival functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

fetch_node_name() ->
    lookup_env_var(?ENV_NODE_NAME).

fetch_device_definitions_dir() ->
    lookup_env_var(?ENV_DEVICE_DEF_DIR).

fetch_deployment_definitions_dir() ->
    lookup_env_var(?ENV_DEPLOYMENT_DEF_DIR).

fetch_scenario_definitions_dir() ->
    lookup_env_var(?ENV_SCENARIO_DEF_DIR).

fetch_selected_scenario() ->
    lookup_env_var(?ENV_SELECTED_SCENARIO).

lookup_env_var(VariableName) ->
    ApplicationName = persistent_term:get({?MODULE, app_name}),
    application:get_env(ApplicationName, VariableName).

fetch_scenario_duration() ->
    {ok, ScenarioName} = fetch_selected_scenario(),
    {ok, {Duration, Units}} = fetch_property_for_scenario(ScenarioName, ?SCENARIO_DURATION_PROP),

    case Units of
        milliseconds -> {ok, Duration};
        seconds -> {ok, Duration * 1000};
        minutes -> {ok, Duration * 60 * 1000};
        _ -> {error, unknown_units}
    end.

fetch_protocol_type() ->
    {ok, ScenarioName} = fetch_selected_scenario(),
    fetch_property_for_scenario(ScenarioName, ?SCENARIO_PROTOCOL_PROP).

fetch_mqtt_client_module() ->
    fetch_protocol_property(?MQTT_CLIENT_INTERFACE_MODULE_PROP, ?MQTT_DEFAULT_CLIENT_INTERFACE_MODULE).

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

fetch_dds_nif_information() ->
    {ok, NifModule} = fetch_protocol_property(?DDS_NIF_MODULE_PROP, ?DDS_DEFAULT_NIF_MODULE),
    {ok, NifFullPath} = fetch_protocol_property(?DDS_NIF_FULL_PATH_PROP, ?DDS_DEFAULT_NIF_FULL_PATH),
    {ok, NifModule, NifFullPath}.

fetch_dds_domain_id() ->
    fetch_protocol_property(?DDS_DOMAIN_ID_PROP).

fetch_dds_config_file_path() ->
    fetch_protocol_property(?DDS_CONFIG_FILE_PATH_PROP, ?DDS_DEFAULT_CONFIG_FILE_PATH).

fetch_dds_qos_profile() ->
    fetch_protocol_property(?DDS_QOS_PROFILE_PROP, ?DDS_DEFAULT_PROFILE).

fetch_deployment_name() ->
    {ok, ScenarioName} = fetch_selected_scenario(),
    fetch_property_for_scenario(ScenarioName, ?SCENARIO_DEPLOYMENT_NAME_PROP).

fetch_node_name_list() ->
    {ok, DeploymentName} = fetch_deployment_name(),
   
    % Node list is saved directly under the deployment name, no properties needed
    NodeList = persistent_term:get({?MODULE, DeploymentName, node_names}, unknown_deployment),
    case NodeList of 
        unknown_deployment -> 
            {error, unknown_deployment};
        _ -> 
            {ok, NodeList}
    end.

fetch_host_for_node_name(NodeName) ->
    fetch_host_for_node_name(NodeName, undefined).

fetch_host_for_node_name(NodeName, DefaultHost) ->
    {ok, ThisNode} = fetch_node_name(),
    case NodeName of
        Value when Value =:= ThisNode ->
            case lookup_env_var("NODE_HOST_OVERRIDE") of
               undefined ->
                    case string:tokens(atom_to_list(node()), "@") of
                        [_Name, H] -> {ok, H};
                        _  -> {ok, DefaultHost}
                    end;
                {ok, Override} ->
                    {ok, Override}
            end;
        _ ->
            fetch_property_for_node(NodeName, ?SCENARIO_HOST_HOSTNAME_PROP, DefaultHost)
    end.

fetch_rng_seed_for_node(NodeName) ->
    fetch_property_for_node(NodeName, ?SCENARIO_HOST_RNG_SEED_PROP).

fetch_property_for_node(NodeName, PropName) ->
    fetch_property_for_node(NodeName, PropName, undefined).

fetch_property_for_node(NodeName, PropName, DefaultValue) ->
    {ok, ScenarioName} = fetch_selected_scenario(),
    {ok, AllNodePropLists} = fetch_property_for_scenario(ScenarioName, ?SCENARIO_HOSTS_PROP),
    
    case proplists:get_value(NodeName, AllNodePropLists) of
        undefined -> 
            {error, unknown_node};
        NodeProps ->
            case proplists:get_value(PropName, NodeProps, DefaultValue) of
                undefined -> 
                    {error, unknown_property};
                Value ->
                    {ok, Value}
            end
    end.

fetch_node_list() ->
    % Check to see if we've already made this
    case persistent_term:get({?MODULE, node_list}, undefined) of
        undefined ->
            {ok, NodeNames} = ps_bench_config_manager:fetch_node_name_list(),
            NodeList = lists:map(fun(X) -> fetch_full_node_from_name(X) end, NodeNames),
            persistent_term:put({?MODULE, node_list}, NodeList),
            {ok, NodeList};
        NodeList ->
            {ok, NodeList}
    end.

fetch_full_node_from_name(NodeName) ->
    {ok, Host} = fetch_host_for_node_name(NodeName, net_adm:localhost()),
    NodeStr = ps_bench_utils:convert_to_list(NodeName),
    HostStr = ps_bench_utils:convert_to_list(Host),
    
    % Build the node name to match current distribution mode
    case node() of
        'nonode@nohost' ->
            % Not distributed yet, check env var to match what will be used
            case os:getenv("ERLANG_DIST_MODE") of
                "longnames" -> 
                    list_to_atom(NodeStr ++ "@" ++ HostStr);
                "name" -> 
                    list_to_atom(NodeStr ++ "@" ++ HostStr);
                _ -> 
                    % Default to shortnames
                    ShortHost = hd(string:tokens(HostStr, ".")),
                    list_to_atom(NodeStr ++ "@" ++ ShortHost)
            end;
        CurrentNode ->
            % Match the format of the current node
            case string:tokens(atom_to_list(CurrentNode), "@") of
                [_CurrentName, CurrentHost] when length(CurrentHost) > 0 ->
                    % We're distributed, use same host format
                    % If CurrentHost has dots, we're using -name, otherwise -sname
                    case string:chr(CurrentHost, $.) of
                        0 -> 
                            % No dots, using -sname, use short hostname
                            ShortHost = hd(string:tokens(HostStr, ".")),
                            list_to_atom(NodeStr ++ "@" ++ ShortHost);
                        _ ->
                            % Has dots, using -name, use full hostname
                            list_to_atom(NodeStr ++ "@" ++ HostStr)
                    end;
                _ ->
                    % Shouldn't happen, fallback
                    list_to_atom(NodeStr ++ "@" ++ HostStr)
            end
    end.
    
fetch_metrics_output_dir() ->
    fetch_metric_property(?METRIC_RESULTS_DIR_PROP, ?DEFAULT_OUT_DIR).

using_hw_poll() ->
    case fetch_metric_property(?METRIC_HW_STATS_POLL_PERIOD, undefined) of
        {error, unknown_property} ->
            false;
        _ ->
            true
    end.

fetch_metric_hw_poll_period() ->
    fetch_metric_property(?METRIC_HW_STATS_POLL_PERIOD).

fetch_python_metric_engine_path() ->
    fetch_metric_property(?METRIC_PYTHON_ENGINE_PATH, ?DEFAULT_PYTHON_ENGINE_PATH).

fetch_python_metric_plugins() ->
    {ok, AllPlugins} = fetch_metric_property(?METRIC_PLUGINS_PROP),
    PythonPlugins = lists:filtermap(fun({Name, Interface}) -> case Interface of ?PYTHON_INTERFACE -> {true, Name}; _ -> false end end, AllPlugins),
    {ok, PythonPlugins}.

fetch_erlang_metric_plugins() ->
    {ok, AllPlugins} = fetch_metric_property(?METRIC_PLUGINS_PROP),
    ErlangPlugins = lists:filtermap(fun({Name, Interface}) -> case Interface of ?ERLANG_INTERFACE -> {true, Name}; _ -> false end end, AllPlugins),
    {ok, ErlangPlugins}.

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
    fetch_protocol_property(Key, undefined).

fetch_protocol_property(Key, DefaultValue) ->
    {ok, ScenarioName} = fetch_selected_scenario(),
    {ok, Protocol} = fetch_protocol_type(),
    ProtocolProps = persistent_term:get({?MODULE, ScenarioName, Protocol}),
    case proplists:get_value(Key, ProtocolProps, DefaultValue) of
        undefined -> 
            {error, unknown_property};
        Value ->
            {ok, Value}
    end.

fetch_metric_property(Key) ->
    fetch_metric_property(Key, undefined).

fetch_metric_property(Key, Default) ->
    {ok, ScenarioName} = fetch_selected_scenario(),
    MetricProps = persistent_term:get({?MODULE, ScenarioName, ?METRIC_STORAGE_CONSTANT}),
    case proplists:get_value(Key, MetricProps, Default) of
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