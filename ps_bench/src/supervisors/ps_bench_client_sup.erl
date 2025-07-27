-module(ps_bench_client_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

-define(WORKER_MODULE, ps_bench_default_erlang_client).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    % Get test name
    {ok, TestName} = application:get_env(test_name),

    Template = #{id => client_worker,
                 start => {?WORKER_MODULE, start_link, [TestName]},
                 restart => transient, 
                 shutdown => 5000,
                 type => worker, 
                 modules => [?WORKER_MODULE]},
    % TODO: Tune or allow configuration of the timing and restart parameters
    {ok, {{simple_one_for_one, 10, 60}, [Template]}}.