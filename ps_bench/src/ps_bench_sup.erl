%%%-------------------------------------------------------------------
%% @doc top-level supervisor
%%%-------------------------------------------------------------------
-module(ps_bench_sup).
-behaviour(supervisor).

-export([start_link/1]).
-export([init/1]).
-export([start_benchmark/0]).

start_link(NodeName) ->
    supervisor:start_link({global, NodeName}, ?MODULE, []).

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

    #{id => ps_bench_test_sup,
    start => {ps_bench_test_sup, start_link, []},
    restart => permanent, shutdown => 5000, % TODO Tweak values
    type => supervisor, modules => [ps_bench_test_sup]}
    ],
    % TODO Tweak values
    {ok, {{one_for_one, 1, 60}, Children}}.

start_benchmark() ->
  ps_bench_test_sup:start_test().

