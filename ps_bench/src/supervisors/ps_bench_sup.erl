%%%-------------------------------------------------------------------
%% @doc top-level supervisor
%%%-------------------------------------------------------------------
-module(ps_bench_sup).
-behaviour(supervisor).

-export([start_link/2]).
-export([init/1]).
-export([start_benchmark/1]).

start_link(NodeName, NodeList) ->
    supervisor:start_link({global, NodeName}, ?MODULE, [{NodeName, NodeList}]).

init([{NodeName, NodeList}]) ->
    MetricsListener = #{id => ps_bench_metrics_listener,
        start => {ps_bench_metrics_listener, start_link, []},
        restart => permanent, shutdown => 5000, type => worker, modules => [ps_bench_metrics_listener]},

    MetricsPy = #{id => ps_bench_metrics_py,
        start => {ps_bench_metrics_py, start_link, [[{listener_name, ps_bench_metrics_listener}]]},
        restart => permanent, shutdown => 5000, type => worker, modules => [ps_bench_metrics_py]},

    Roll = #{id => ps_bench_metrics_rollup,
        start => {ps_bench_metrics_rollup, start_link, []},
        restart => permanent, shutdown => 5000, type => worker, modules => [ps_bench_metrics_rollup]},

    Store = #{id => ps_bench_store,
        start => {ps_bench_store, start_link, []},
        restart => permanent, shutdown => 5000, type => worker, modules => [ps_bench_store]},

    %% existing children
    Pg = #{id => pg_srv, start => {pg, start_link, []},
        restart => permanent, shutdown => 5000, type => worker, modules => [pg]},

    Lifecycle = #{id => ps_bench_lifecycle,
        start => {ps_bench_lifecycle, start_link, [NodeList, 5000]},
        restart => permanent, shutdown => 5000, type => worker, modules => [ps_bench_lifecycle]},

    Manager = #{id => ps_bench_manager,
        start => {ps_bench_manager, start_link, [NodeName]},
        restart => permanent, shutdown => 5000, type => worker, modules => [ps_bench_manager]},

    TestSup = #{id => ps_bench_test_sup,
        start => {ps_bench_test_sup, start_link, []},
        restart => permanent, shutdown => 5000, type => supervisor, modules => [ps_bench_test_sup]},

    Children = [Pg, Store, MetricsListener, MetricsPy, Lifecycle, Manager, TestSup],
    {ok, {{one_for_one, 5, 60}, Children}}.


start_benchmark(NodeName) ->
  ps_bench_lifecycle:current_step_complete(NodeName),
  ps_bench_test_sup:start_test().

