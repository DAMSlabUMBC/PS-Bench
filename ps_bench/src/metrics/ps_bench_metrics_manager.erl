-module(ps_bench_metrics_manager).

-include("ps_bench_config.hrl").

-export([initialize_plugins/0, run_metric_calculations/0]).

initialize_plugins() ->
    ps_bench_utils:log_state_change("Initializing Metric Calculation Plugins", []),

    % Fetch output folder and make sure it can be created or exists
    {ok, OutDir} = ps_bench_config_manager:fetch_metrics_output_dir(),
    
    % Create timestamped subdirectory for this run
    {{Year, Month, Day}, {Hour, Minute, Second}} = calendar:local_time(),
    Timestamp = io_lib:format("~4..0B~2..0B~2..0B_~2..0B~2..0B~2..0B", 
                              [Year, Month, Day, Hour, Minute, Second]),
    {ok, ScenarioName} = ps_bench_config_manager:fetch_selected_scenario(),
    RunDir = filename:join([OutDir, lists:flatten(["run_", Timestamp, "_", atom_to_list(ScenarioName)])]),
    ok = filelib:ensure_dir(RunDir ++ "/"),
    
    % Store the run directory for later use
    persistent_term:put({?MODULE, run_dir}, RunDir),

    % Initialize the plugins with the timestamped directory
    initialize_erlang_plugins(RunDir).

initialize_erlang_plugins(OutDir) ->

    % Fetch and initialize the plugins
    {ok, Plugins} = ps_bench_config_manager:fetch_erlang_metric_plugins(),
    InitFunction = fun(ModuleName) -> ModuleName:init(OutDir) end,
    lists:foreach(InitFunction, Plugins),
    ps_bench_utils:log_message("Initialized Erlang metric plugins: ~p", [Plugins]).

initialize_python_plugins(OutDir) ->

    %% Build an absolute path to priv/py_engine that works in dev & release
    PrivDir = code:priv_dir(ps_bench),
    PyPath  = filename:join(PrivDir, "py_engine"),

    % Fetch plugins
    {ok, Plugins} = ps_bench_config_manager:fetch_python_metric_plugins(),
    
    BinaryOutDir  = unicode:characters_to_binary(OutDir),
    {ok, Py} = python:start_link([{python_path, [PyPath]}, {python, ?DEFAULT_PYTHON_EXECUTABLE}]),
    persistent_term:put({?MODULE, python_engine}, Py),

    % Now call into python to initalize the plugins
    ok = python:call(Py, plugin_engine, init, [Plugins, BinaryOutDir]),
    ps_bench_utils:log_message("Initialized Python metric plugins: ~p", [Plugins]).

run_metric_calculations() ->
    run_python_plugins(),
    run_erlang_plugins(),

    % Get the stored run directory instead of the base directory
    RunDir = persistent_term:get({?MODULE, run_dir}, undefined),
    OutDir = case RunDir of
        undefined -> 
            % Fallback to base directory if no run dir was stored
            {ok, BaseDir} = ps_bench_config_manager:fetch_metrics_output_dir(),
            BaseDir;
        _ -> RunDir
    end,

    % Write hardware stats if we're using it
    case ps_bench_config_manager:using_hw_poll() of
        true ->
            gen_server:call(ps_bench_metrics_hw_stats_reader, {write_stats, OutDir});
        false ->
            ok
    end.

run_python_plugins() ->

    % This is a future feature but not yet implemented

    % Py = persistent_term:get({?MODULE, python_engine}),

    % %% Fetch stored metric data and send to each python plugin
    % ps_bench_utils:log_message("Fetching recv events", []),
    % RecvEvents = ps_bench_store:fetch_recv_events(),
    % ps_bench_utils:log_message("Fetching pub events", []),
    % PublishEvents = ps_bench_store:fetch_publish_events(),
    % ps_bench_utils:log_message("Fetching connect events", []),
    % ConnectEvents = ps_bench_store:fetch_connect_events(),
    % ps_bench_utils:log_message("Fetching disconnect events", []),
    % DisconnectEvents = ps_bench_store:fetch_disconnect_events(),

    % {ok, ThisNodeName} = ps_bench_config_manager:fetch_node_name(),
    % {ok, AllNodes} = ps_bench_config_manager:fetch_node_name_list(),

    % Result = python:call(Py, plugin_engine, calc_runner_metrics, [RecvEvents, PublishEvents, ConnectEvents, DisconnectEvents, ThisNodeName, AllNodes]),
    % Result = python:call(Py, plugin_engine, calc_runner_metrics, [[], [], [], [], [], []]),
    % ps_bench_utils:log_message("Python call result ~p", [Result]),
    ok.

run_erlang_plugins() ->
    {ok, Plugins} = ps_bench_config_manager:fetch_erlang_metric_plugins(),
    CalcFunction = fun(ModuleName) -> ps_bench_utils:log_message("Calculating runner metrics with erlang plugin ~p", [ModuleName]), ModuleName:calc() end,
    lists:foreach(CalcFunction, Plugins).