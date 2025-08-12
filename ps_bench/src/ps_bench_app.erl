%%%-------------------------------------------------------------------
%% @doc bench_ctrl public API
%% @end
%%%-------------------------------------------------------------------

-module(ps_bench_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->

    io:format("=== Loading Config ===~n"),
    case ps_bench_config_manager:load_config_from_env_vars() of
        ok ->
            io:format("=== Initializing Benchmark ===~n"),

            % Initialize random number generator
            ps_bench_utils:initialize_rng_seed(), % TODO, need to sync across all nodes and allow loading from config

            {ok, NodeName} = ps_bench_config_manager:fetch_node_name(),
            io:format("~p starting~n", [NodeName]),

            {ok, NodeList} = ps_bench_config_manager:fetch_node_list(),

            {ok, TopSupPid} = ps_bench_sup:start_link(NodeName, NodeList),
            ps_bench_sup:start_benchmark(NodeName),
            {ok, TopSupPid};
        {error, Reason} ->
            {error, Reason}
    end.
    

stop(_State) ->
    ok.

%% internal functions
