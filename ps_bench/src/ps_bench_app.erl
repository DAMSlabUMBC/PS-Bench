%%%-------------------------------------------------------------------
%% @doc bench_ctrl public API
%% @end
%%%-------------------------------------------------------------------

-module(ps_bench_app).

-behaviour(application).

-export([start/2, stop/1]).
-export([stop_benchmark_application/0]).

start(_StartType, _StartArgs) ->

    case ps_bench_config_manager:load_config_from_env_vars() of
        ok ->   
            % Fetch required information then start the supervisor
            {ok, NodeName} = ps_bench_config_manager:fetch_node_name(),
            {ok, NodeList} = ps_bench_config_manager:fetch_node_name_list(),
            {ok, TopSupPid} = ps_bench_sup:start_link(NodeName, NodeList),

            % Now that the supervision tree is started, tell the benchmark to initialize itself
            ps_bench_node_manager:setup_benchmark(),
            {ok, TopSupPid};
        {error, Reason} ->
            {error, Reason}
    end.
    

stop(_State) ->
    ok.

stop_benchmark_application() ->

    ps_bench_utils:log_state_change("Exiting Benchmark"),

    {ok, ApplicationName} = application:get_application(),
    error_logger:tty(false),
    application:stop(ApplicationName),
    error_logger:tty(true).