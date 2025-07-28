-module(ps_bench_config_manager).

-include("ps_bench_config.hrl").

%% public
-export([load_config_from_env_vars/0, fetch_property_for_device/2, fetch_node_name/0, fetch_test_name/0, fetch_node_list/0,
    fetch_protocol_type/0, fetch_client_interface_information/0, fetch_protocol_property/1, fetch_devices_for_this_node/0]).

% =====================================================================
% Loading functions
% =====================================================================
load_config_from_env_vars() ->
    case ensure_env_vars_set() of
        ok ->
            {ok, DeviceDefDir} = fetch_device_definitions_dir(),
            {ok, TestConfigPath} = fetch_config_file_path(),
            load_config(DeviceDefDir, TestConfigPath);
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


load_config(DeviceDefDir, TestConfigPath) ->

    case load_device_definitions(DeviceDefDir) of
        ok ->
            case load_test_config(TestConfigPath) of
                ok ->
                    ok;
                error ->
                    {error, test_config_load_failed}
            end;
        error ->
            {error, device_load_failed}
    end.

load_device_definitions(DirPath) ->

    case filelib:is_dir(DirPath) of
        true ->
            io:format("Loading device definitions from ~p...~n", [DirPath]),
            DeviceDefs = filelib:wildcard(?DEVICE_FILE_EXT, DirPath),
            FullDevicePaths =  lists:map(fun(DefFile) -> string:join([DirPath, DefFile], "/") end, DeviceDefs),
            lists:foreach(fun load_device_definition/1, FullDevicePaths),
            io:format("Done loading device definitions.~n"),
            ok;
        false ->
            io:format("Provided device definitions path was not found or is not a directory: ~p~n", [DirPath]),
            error
    end.

load_device_definition(FilePath) ->

    io:format("\tLoading ~s...~n", [filename:basename(FilePath)]),
    
    case file:consult(FilePath) of
        {ok, [Def]} ->
            case process_device_definition(Def) of
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

load_test_config(FilePath) ->

    io:format("Loading test config from ~p...~n", [FilePath]),

    case file:consult(FilePath) of
        {ok, [Terms]} ->
            case process_test_config(Terms) of
                ok ->
                    io:format("> Success!~n", []),
                    ok;
                {error, Reason} ->
                    io:format("> Failed: ~s~n", [Reason]),
                    error
            end;

        {error, {Line, _Mod, Term}} ->
            io:format("> Failed: Config file malformed on line ~p: ~p~n", [Line, Term]),
            error;

        {error, enoent} ->
            io:format("> Test configuration file not found at ~p~n", [FilePath]),
            error;

        {error, Error} ->
            io:format("> Could not read test configuration at ~p. Error: ~p~n", [FilePath, Error]),
            error
    end.

process_test_config(PropList) ->
    
    % First process the global section
    case process_global_properties(PropList) of
        ok ->
            % Now process this node's section
            process_local_node_config(PropList);
        {error, Reason} ->
            {error, Reason}
    end.

process_global_properties(PropList) ->

    case proplists:get_value(?TEST_CONFIG_GLOBAL_SECTION_PROP, PropList) of
        undefined ->
            Reason = io_lib:format("Missing required property ~p.", [?TEST_CONFIG_GLOBAL_SECTION_PROP]),
            {error, Reason};
        GlobalPropList ->
                % Validate needed keys are here
                GlobalPropListKeys = proplists:get_keys(GlobalPropList),
                MissingKeys = ?TEST_CONFIG_GLOBAL_REQ_KEY_LIST -- GlobalPropListKeys,

                case MissingKeys of
                    [] ->
                        % Extract the required global keys and store
                        % (Note this will remove the protocol section, which we want here)
                        PropsToSave = lists:filter(fun({Key, _Value}) -> lists:member(Key, ?TEST_CONFIG_GLOBAL_REQ_KEY_LIST) end, GlobalPropList),
                        ok = persistent_term:put({?MODULE, ?TEST_CONFIG_GLOBAL_SECTION_PROP}, PropsToSave),

                        % Then parse protocol config
                        process_protocol_config(GlobalPropList);
                    _ ->
                        Reason = io_lib:format("Missings key(s) ~p in test config.", [MissingKeys]),
                        {error, Reason}
                end
    end.

process_protocol_config(GlobalPropList) ->
    Protocol = proplists:get_value(?TEST_PROTOCOL_PROP, GlobalPropList),

    % Make sure protocol is supported
    case lists:member(Protocol, ?SUPPORTED_PROTOCOLS) of
    true ->

        % Make sure protocol section is here
        case proplists:is_defined(Protocol, GlobalPropList) of 
            true ->

                % Process properties in protocol section based on protocol
                ProtocolPropList = proplists:get_value(Protocol, GlobalPropList),
                case Protocol of 
                    ?MQTT_V5_PROTOCOL ->
                        process_mqtt_config(ProtocolPropList, Protocol);
                    ?MQTT_V311_PROTOCOL ->
                        process_mqtt_config(ProtocolPropList, Protocol);
                    ?DDS_PROTOCOL ->
                        process_dds_config(ProtocolPropList)
                end;

            false ->
                Reason = io_lib:format("Protocol ~p requested, but no configuration given.", [Protocol]),
                {error, Reason}
        end;

    false ->
        Reason = io_lib:format("Unsupported protocol ~p requested.", [Protocol]),
        {error, Reason}
end.

process_mqtt_config(ProtocolPropList, ProtocolVersion) ->
    % Validate needed keys are here
    PropListKeys = proplists:get_keys(ProtocolPropList),
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
            persistent_term:put({?MODULE, ProtocolVersion}, ProtocolPropList);
        _ ->
            Reason = io_lib:format("Missings key(s) ~p in MQTT properties.", [MissingKeys]),
            {error, Reason}
    end.

process_dds_config(_ProtocolPropList) ->
    Reason = io_lib:format("DDS not currently supported.", []),
    {error, Reason}.

process_local_node_config(PropList) ->
    {ok, ThisNode} = fetch_node_name(),
    case proplists:get_value(ThisNode, PropList) of
        undefined ->
            Reason = io_lib:format("Missing node configuration for node ~p. Check the node name is correctly set as an env variable (Is it an atom?).", [ThisNode]),
            {error, Reason};
        NodePropList ->
                % Validate needed keys are here
                NodePropListKeys = proplists:get_keys(NodePropList),
                MissingKeys = ?TEST_CONFIG_NODE_REQ_KEY_LIST -- NodePropListKeys,

                case MissingKeys of
                    [] ->
                        % Extract the required keys and store
                        PropsToSave = lists:filter(fun({Key, _Value}) -> lists:member(Key, ?TEST_CONFIG_NODE_REQ_KEY_LIST) end, NodePropList),
                        persistent_term:put({?MODULE, ThisNode}, PropsToSave);
                    _ ->
                        Reason = io_lib:format("Missings key(s) ~p in test config for node ~p.", [MissingKeys, ThisNode]),
                        {error, Reason}
                end
    end.

% =====================================================================
% Retrival functions
% =====================================================================
fetch_node_name() ->
    application:get_env(?ENV_NODE_NAME).

fetch_device_definitions_dir() ->
    application:get_env(?ENV_DEVICE_DEF_DIR).

fetch_config_file_path() ->
    application:get_env(?ENV_TEST_CONFIG_FILE).

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

fetch_test_name() ->
    get_global_property(?TEST_NAME_PROP).

fetch_node_list() ->
    get_global_property(?TEST_NODE_LIST_PROP).

fetch_protocol_type() ->
    get_global_property(?TEST_PROTOCOL_PROP).

fetch_client_interface_information() ->
    % This may not exist so we specify a default value
    {ok, InterfaceType} = get_global_property(?TEST_INTERFACE_TYPE_PROP, ?DEFAULT_INTERFACE_TYPE),
    {ok, InterfaceName} = get_global_property(?TEST_INTERFACE_NAME_PROP, ?DEFAULT_INTERFACE_NAME),
    {ok, InterfaceType, InterfaceName}.

fetch_protocol_property(Key) ->
    {ok, Protocol} = fetch_protocol_type(),
    PropList = get_persistent_term(Protocol),
    case proplists:get_value(Key, PropList) of
        undefined -> 
            {error, unknown_property};
        Value ->
            {ok, Value}
    end.

fetch_devices_for_this_node() ->
    get_node_property(?TEST_NODE_DEVICES_PROP).

get_global_property(PropName) ->
    PropList = get_persistent_term(?TEST_CONFIG_GLOBAL_SECTION_PROP),
    case proplists:get_value(PropName, PropList) of 
        undefined -> 
            {error, unknown_property};
        Value ->
            {ok, Value}
    end.

get_global_property(PropName, Default) ->
    PropList = get_persistent_term(?TEST_CONFIG_GLOBAL_SECTION_PROP),
    Value = proplists:get_value(PropName, PropList, Default),
    {ok, Value}.

get_node_property(PropName) ->
    {ok, ThisNode} = fetch_node_name(),
    PropList = get_persistent_term(ThisNode),
    case proplists:get_value(PropName, PropList) of
        undefined ->
            {error, unknown_property};
        Value ->
            {ok, Value}
    end.

get_persistent_term(Key) ->
    persistent_term:get({?MODULE, Key}).