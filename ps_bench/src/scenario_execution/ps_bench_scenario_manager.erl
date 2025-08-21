-module(ps_bench_scenario_manager).

-include("ps_bench_config.hrl").

-define(CLIENT_SUPERVISOR, ps_bench_client_sup).
-define(NODE_MANAGER, ps_bench_node_manager).

%% public
-export([initialize_scenario/0, run_scenario/0, stop_scenario/0]).

% Currently we only support one scenario a run
initialize_scenario() ->
    
    {ok, ScenarioName} = ps_bench_config_manager:fetch_selected_scenario(),
    ps_bench_utils:log_state_change("Initializing Scenario: ~p", [ScenarioName]),
    
    initialize_clients(),
    print_clients(),
    connect_clients(),
    subscribe_clients(),
    timer:sleep(1000).

run_scenario() ->

    {ok, ScenarioName} = ps_bench_config_manager:fetch_selected_scenario(),
    ps_bench_utils:log_state_change("Starting Scenario: ~p", [ScenarioName]),

    {ok, DurationMs} = ps_bench_config_manager:fetch_scenario_duration(),

    % Display time and duration for user
    {{Y,M,D},{H,MM,SS}} = erlang:localtime(),
    TimeStr = lists:flatten(io_lib:format("~B-~2.10.0B-~2.10.0B ~2.10.0B:~2.10.0B:~2.10.0B", [Y, M, D,H,MM,SS])),
    ps_bench_utils:log_message("Starting at ~s and running for ~pms", [TimeStr, DurationMs]),

    start_client_loops(),
    timer:apply_after(DurationMs, fun stop_scenario/0).

stop_scenario() ->
    {ok, ScenarioName} = ps_bench_config_manager:fetch_selected_scenario(),
    ps_bench_utils:log_state_change("Stopping Scenario: ~p", [ScenarioName]),

    % Stop clients and notify the benchmarking scenario is complete
    stop_clients(),
    destroy_clients(),
    gen_server:cast(?NODE_MANAGER, global_continue).

initialize_clients() ->

    % Fetch devices for this node
    {ok, NodeDevices} = ps_bench_config_manager:fetch_devices_for_this_node(),
    {ok, NodeName} = ps_bench_config_manager:fetch_node_name(),

    % Initialize each client for each device
    ClientList = initialize_clients_for_devices(NodeName, NodeDevices, []),

    % Store the list of client PIDs using the persistent_term module which has constant speed lookup
    persistent_term:put(?MODULE, ClientList).

initialize_clients_for_devices(_NodeName, [], ClientList) ->
    ClientList;

initialize_clients_for_devices(NodeName, [{DeviceType, DeviceCount} | NodeDevices] , ClientList) ->
    
    % Initialize the device clients
    ClientListForDevice = initialize_client_for_device_type(NodeName, DeviceType, DeviceCount, []),

    % Reverse the list just to put it in index order
    NewClientList = ClientList ++ lists:reverse(ClientListForDevice),

    % Recursively process the rest of the devices
    initialize_clients_for_devices(NodeName, NodeDevices, NewClientList).

initialize_client_for_device_type(NodeName, DeviceType, DevicesToCreateCount, DeviceClientList) ->
    % Construct name and start child
    StringNodeName = atom_to_list(NodeName),
    ThisDeviceIndex = length(DeviceClientList) + 1,
    ClientName = StringNodeName ++ "_" ++ atom_to_list(DeviceType) ++ "_" ++ integer_to_list(ThisDeviceIndex),
    {ok, ClientPid} = supervisor:start_child(?CLIENT_SUPERVISOR, [ClientName, DeviceType]),
    NewDeviceClientList = [{ClientName, ClientPid} | DeviceClientList],

    if ThisDeviceIndex < DevicesToCreateCount -> 
            initialize_client_for_device_type(NodeName, DeviceType, DevicesToCreateCount, NewDeviceClientList);
       ThisDeviceIndex =:= DevicesToCreateCount -> 
            NewDeviceClientList
    end.

connect_clients() ->
    ClientList = persistent_term:get(?MODULE),
    ConnectFunction = fun({_ClientName, ClientPid}) -> gen_server:call(ClientPid, connect) end,
    lists:foreach(ConnectFunction, ClientList).

subscribe_clients() ->
    ClientList = persistent_term:get(?MODULE),
    SubscribeFunction = fun({_ClientName, ClientPid}) -> gen_server:call(ClientPid, subscribe) end,
    lists:foreach(SubscribeFunction, ClientList).

start_client_loops() ->
    ClientList = persistent_term:get(?MODULE),
    StartFunction = fun({_ClientName, ClientPid}) -> gen_server:cast(ClientPid, start_client_loops) end,
    lists:foreach(StartFunction, ClientList).

stop_clients() ->
    ClientList = persistent_term:get(?MODULE),
    StopFunction = fun({_ClientName, ClientPid}) -> gen_server:cast(ClientPid, stop) end,
    lists:foreach(StopFunction, ClientList).

destroy_clients() ->
    ClientList = persistent_term:get(?MODULE),
    DestroyFunction = fun({_ClientName, ClientPid}) -> supervisor:terminate_child(?CLIENT_SUPERVISOR, ClientPid) end,
    lists:foreach(DestroyFunction, ClientList),
    persistent_term:erase(?MODULE).

print_clients() ->
    ClientList = persistent_term:get(?MODULE),
    ps_bench_utils:log_state_change("All Clients"),
    PrintFunction = fun({ClientName, ClientPid}) -> ps_bench_utils:log_message("~p - ~p", [ClientName, ClientPid]) end,
    lists:foreach(PrintFunction, ClientList).