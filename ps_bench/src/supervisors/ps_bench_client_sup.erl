-module(ps_bench_client_sup).
-behaviour(supervisor).

-include("ps_bench_config.hrl").

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, ScenarioName} = ps_bench_config_manager:fetch_selected_scenario(),
    {ok, InterfaceType, InterfaceName} = ps_bench_config_manager:fetch_client_interface_information(),

    % Need to determine which type of client to start
    case InterfaceType of
        ?ERLANG_INTERFACE ->
            Template = #{id => client_worker,
                        start => {ps_bench_erlang_mqtt_adapter, start_link, [ScenarioName, InterfaceName]}, % Two more parameters are added during start_child
                        restart => transient, 
                        shutdown => 5000,
                        type => worker, 
                        modules => [ps_bench_erlang_mqtt_adapter]},
            % TODO: Tune or allow configuration of the timing and restart parameters
            {ok, {{simple_one_for_one, 10, 60}, [Template]}};
        ?PYTHON_INTERFACE ->
            ps_bench_utils:log_message("ERROR: Python client interfaces are currently not supported"),
            {error, unsupported_interface};
        _ ->
            ps_bench_utils:log_message("ERROR: Unknown interface ~p", [InterfaceType]),
            {error, unsupported_interface}
    end.