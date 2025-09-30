%%%-------------------------------------------------------------------
%% @doc top-level supervisor
%%%-------------------------------------------------------------------
-module(ps_bench_sup).
-behaviour(supervisor).

-export([start_link/2]).
-export([init/1]).

start_link(NodeName, NodeList) ->
    supervisor:start_link({global, NodeName}, ?MODULE, [{NodeName, NodeList}]).

init([{NodeName, NodeList}]) ->
    Pg = #{id => pg_srv, start => {pg, start_link, []},
        restart => permanent, shutdown => 5000, type => worker, modules => [pg]},

    Lifecycle = #{id => ps_bench_lifecycle,
        start => {ps_bench_lifecycle, start_link, [NodeList, 100000]},
        restart => permanent, shutdown => 5000, type => worker, modules => [ps_bench_lifecycle]},

    NodeManager = #{id => ps_bench_node_manager,
        start => {ps_bench_node_manager, start_link, [NodeName]},
        restart => permanent, shutdown => 5000, type => worker, modules => [ps_bench_node_manager]},

    ScenarioSup = #{id => ps_bench_scenario_sup,
        start => {ps_bench_scenario_sup, start_link, []},
        restart => permanent, shutdown => 5000, type => supervisor, modules => [ps_bench_scenario_sup]},

    HwStats = #{id => ps_bench_metrics_hw_stats_reader,
        start => {ps_bench_metrics_hw_stats_reader, start_link, []},
        restart => permanent, shutdown => 5000, type => worker, modules => [ps_bench_metrics_hw_stats_reader]},

    Children = case ps_bench_config_manager:using_hw_poll() of
        true ->
            [Pg, Lifecycle, NodeManager, ScenarioSup, HwStats];
        false ->
            [Pg, Lifecycle, NodeManager, ScenarioSup]
    end,
    {ok, {{one_for_one, 5, 60}, Children}}.

