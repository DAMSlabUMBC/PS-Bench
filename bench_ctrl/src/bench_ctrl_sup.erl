%%%-------------------------------------------------------------------
%% @doc top-level supervisor
%%%-------------------------------------------------------------------
-module(bench_ctrl_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    %% ---------------------------------------------------------------
    %% permanent children
    %% ---------------------------------------------------------------
    Children = [
    %%  pg process – needed once so pg:join/2 works
        #{id       => pg_srv,
          start    => {pg, start_link, []},
          restart  => permanent, shutdown => 5000,
          type     => worker,   modules  => [pg]},
    
    %%  clock sync ----------------------------------------------------
    #{id => clock_sync,
      start => {clock_sync, start_link, []},
      restart => permanent, shutdown => 5000,
      type => worker, modules => [clock_sync]},

    %%  metric aggregator --------------------------------------------
    #{id => metric_agg,
      start => {metric_agg_srv, start_link, []},
      restart => permanent, shutdown => 5000,
      type => worker, modules => [metric_agg_srv]},

    %%  dynamic supervisor (allows extra agents later) ---------------
    #{id => node_agent_sup,
      start => {dynamic_sup, start_link, [node_agent]},
      restart => permanent, shutdown => 5000,
      type => supervisor, modules => [dynamic_sup]},

    %%  **bootstrap one default agent** ------------------------------
    #{id => bootstrap_agent,
      start => {node_agent, start_link, []},
      restart => transient, shutdown => 4000,
      type => worker, modules => [node_agent]}
    ],
    {ok, {{one_for_one, 5, 10}, Children}}.
