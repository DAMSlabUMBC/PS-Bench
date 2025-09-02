-module(ps_bench_client_sup).
-behaviour(supervisor).

-include("ps_bench_config.hrl").

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, ScenarioName} = ps_bench_config_manager:fetch_selected_scenario(),
    {ok, ProtocolType} = ps_bench_config_manager:fetch_protocol_type(),

    % Need to determine which type of client to start
    case ProtocolType of
        ?MQTT_V5_PROTOCOL ->
            process_mqtt_children(ScenarioName);
        ?MQTT_V311_PROTOCOL ->
            process_mqtt_children(ScenarioName);
        ?DDS_PROTOCOL ->
            process_dds_children(ScenarioName);
        _ ->
            ps_bench_utils:log_message("ERROR: Requested starting a child of unknown protocol ~p", [ProtocolType]),
            {error, unsupported_protocol}
    end.

process_mqtt_children(ScenarioName) ->

    {ok, ClientModule} = ps_bench_config_manager:fetch_mqtt_client_module(),
    
    % MQTT curenlty only supports erlang interfaces
    InterfaceType = ?ERLANG_INTERFACE,

    case InterfaceType of
        ?ERLANG_INTERFACE ->
            Template = #{id => client_worker,
                        start => {ps_bench_erlang_mqtt_adapter, start_link, [ScenarioName, ClientModule]}, % Two more parameters are added during start_child
                        restart => transient, 
                        shutdown => 5000,
                        type => worker, 
                        modules => [ps_bench_erlang_mqtt_adapter]},
            {ok, {{simple_one_for_one, 10, 60}, [Template]}};
        _ ->
            ps_bench_utils:log_message("ERROR: Requested starting a MQTT child of unknown interface"),
            {error, unsupported_interface}
    end.

process_dds_children(ScenarioName) ->

    {ok, NifModule, FullNifPath} = ps_bench_config_manager:fetch_dds_nif_information(),
    {ok, DomainId} = ps_bench_config_manager:fetch_dds_domain_id(),

    % DDS curenlty only supports erlang interfaces
    InterfaceType = ?NIF_INTERFACE,

    case InterfaceType of
        ?NIF_INTERFACE ->            
            Template = #{id => client_worker,
                        start => {ps_bench_nif_dds_adapter, start_link, [ScenarioName, NifModule, FullNifPath, DomainId]}, % Two more parameters are added during start_child
                        restart => transient, 
                        shutdown => 5000,
                        type => worker, 
                        modules => [ps_bench_nif_dds_adapter]},
            {ok, {{simple_one_for_one, 10, 60}, [Template]}};
        _ ->
            ps_bench_utils:log_message("ERROR: Requested starting a DDS child of unknown interface"),
            {error, unsupported_interface}
    end.