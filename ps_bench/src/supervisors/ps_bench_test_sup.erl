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
      start     => {ps_bench_client_sup, start_link, []},
      restart   => permanent, 
      shutdown  => 5000, % TODO Tweak values
      type      => supervisor, 
      modules   => [ps_bench_client_sup]}
    ],
    % TODO Tweak values
    {ok, {{one_for_one, 1, 60}, Children}}. 

start_test() ->
    ps_bench_test_manager:initialize_clients(),
    timer:sleep(1000),
    ps_bench_test_manager:print_clients(),
    timer:sleep(1000),
    ps_bench_test_manager:connect_clients(),
    timer:sleep(1000),
    ps_bench_test_manager:subscribe_clients_to_topic(<<"test">>, 0),
    timer:sleep(1000),
    ps_bench_test_manager:publish_data_to_clients(<<"test">>, <<"Data">>, 0),
    timer:sleep(1000),
    ps_bench_test_manager:disconnect_clients(),
    timer:sleep(1000),
    ps_bench_test_manager:reconnect_clients(),
    timer:sleep(1000),
    ps_bench_test_manager:disconnect_clients().

