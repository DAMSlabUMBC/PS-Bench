-module(ps_bench_client_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

-define(WORKER_MODULE, ps_bench_erlang_adapter).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    % Get test name
    {ok, ScenarioName} = ps_bench_config_manager:fetch_selected_scenario(),
    {ok, _InterfaceType, InterfaceName} = ps_bench_config_manager:fetch_client_interface_information(),

    Template = #{id => client_worker,
                 start => {?WORKER_MODULE, start_link, [ScenarioName, InterfaceName]},
                 restart => transient, 
                 shutdown => 5000,
                 type => worker, 
                 modules => [?WORKER_MODULE]},
    % TODO: Tune or allow configuration of the timing and restart parameters
    {ok, {{simple_one_for_one, 10, 60}, [Template]}}.