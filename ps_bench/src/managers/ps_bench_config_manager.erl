-module(ps_bench_config_manager).

-include("ps_bench_config.hrl").

%% public
-export([load_config/2, fetch_property_for_device/2]).

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

load_test_config(FilePath) ->

    io:format("Loading test config from ~p...~n", [FilePath]),

    case file:consult(FilePath) of
        {ok, Terms} ->
            io:format("Read ~p~n", [Terms]),
            ok;

        {error, {Line, _Mod, Term}} ->
            io:format("Config file malformed on line ~p: ~p~n", [Line, Term]),
            error;

        {error, enoent} ->
            io:format("Test configuration file not found at ~p~n", [FilePath]),
            error;

        {error, Error} ->
            io:format("Could not read test configuration at ~p. Error: ~p~n", [FilePath, Error]),
            error
    end.

load_device_definition(FilePath) ->

    io:format("\tLoading ~p...~n", [FilePath]),
    
    case file:consult(FilePath) of
        {ok, [Def]} ->
            case process_device_definition(Def) of
                ok ->
                    io:format("\tSuccess!~n", []),
                    ok;
                {error, Reason} ->
                    io:format("\tFailed: ~s~n", [Reason]),
                    error
            end;

        {error, {Line, _Mod, Term}} ->
            io:format("\tFailed: Malformed on line ~p: ~p~n", [Line, Term]),
            error;

        {error, enoent} ->
            io:format("\tFailed: File not found at ~p~n", [FilePath]),
            error;

        {error, Error} ->
            io:format("\tFailed: Could not read file ~p. Error: ~p~n", [FilePath, Error]),
            error
    end.

process_device_definition(Def) ->

    % Validate needed attributes are here
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
            Type = proplists:get_value(?DEVICE_TYPE_FIELD, Def),
            persistent_term:put({?MODULE, Type}, Def);
        _ ->
            Reason = io_lib:format("Missings key(s) ~p in definition.~n", [MissingKeys]),
            {error, Reason}
    end.

fetch_property_for_device(DeviceType, PropName) ->

    % Retrieve prop list and make sure the device exists
    Def = persistent_term:get({?MODULE, DeviceType}, unknown_device),

    case Def of 
        unknown_device ->
            {error, unknown_device};
        _ ->

            % Now check the property existence
            case proplists:is_defined(PropName, Def) of
                true ->
                    Value = proplists:get_value(PropName, Def),
                    {ok, Value};
                false ->
                    {error, unknown_device_property}
            end
    end.