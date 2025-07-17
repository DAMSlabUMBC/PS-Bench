%%%-------------------------------------------------------------------
%% @doc top-level supervisor
%%%-------------------------------------------------------------------
-module(ps_bench_test_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).
-export([start_test/0]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    %% ---------------------------------------------------------------
    %% permanent children
    %% ---------------------------------------------------------------
    Children = [

    %%  client supervisor ---------------
    #{id => ps_bench_client_sup,
      start => {ps_bench_client_sup, start_link, []},
      restart => permanent, shutdown => 5000, % TODO Tweak values
      type => supervisor, modules => [ps_bench_client_sup]}
    ],
    % TODO Tweak values
    {ok, {{one_for_one, 1, 60}, Children}}. 

start_test() ->
    ps_bench_client_sup:start_clients().
