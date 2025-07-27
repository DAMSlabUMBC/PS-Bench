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
    %% ---------------------------------------------------------------
    %% permanent children
    %% ---------------------------------------------------------------
    Children = [
    %%  pg process – needed once so pg:join/2 works
    #{id       => pg_srv,
      start    => {pg, start_link, []},
      restart  => permanent, 
      shutdown => 5000, % TODO Tweak values
      type     => worker,   
      modules  => [pg]},

    #{id       => ps_bench_lifecycle,
      start    => {ps_bench_lifecycle, start_link, [NodeList, 5000]}, % TODO SET Timeout
      restart  => permanent, 
      shutdown => 5000, % TODO Tweak values
      type     => worker,   
      modules  => [ps_bench_lifecycle]},

    #{id       => ps_bench_manager,
      start    => {ps_bench_manager, start_link, [NodeName]}, % TODO SET Timeout
      restart  => permanent, 
      shutdown => 5000, % TODO Tweak values
      type     => worker,   
      modules  => [ps_bench_lifecycle]},

    #{id       => ps_bench_test_sup,
      start    => {ps_bench_test_sup, start_link, []},
      restart  => permanent, 
      shutdown => 5000, % TODO Tweak values
      type     => supervisor, 
      modules  => [ps_bench_test_sup]}
    ],
    % TODO Tweak values
    {ok, {{one_for_one, 1, 60}, Children}}.

start_benchmark(NodeName) ->
  ps_bench_lifecycle:current_step_complete(NodeName),
  ps_bench_test_sup:start_test().

