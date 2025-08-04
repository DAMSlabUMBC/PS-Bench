-module(ps_bench_test_manager).

-include("ps_bench_config.hrl").

-define(CHILD_MODULE, ps_bench_client_sup).

%% public
-export([initialize_clients/0, print_clients/0, connect_clients/0, start_client_loops/0,
    subscribe_clients/0]).

initialize_clients() ->

    % Fetch configs for this node
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

% This override just lets us use both atoms and strings as node names
initialize_client_for_device_type(NodeName, DeviceType, DevicesToCreateCount, DeviceClientList) when is_atom(NodeName) ->
    initialize_client_for_device_type(atom_to_list(NodeName), DeviceType, DevicesToCreateCount, DeviceClientList);

initialize_client_for_device_type(NodeName, DeviceType, DevicesToCreateCount, DeviceClientList) ->
    % Construct name and start child
    ThisDeviceIndex = length(DeviceClientList) + 1,
    ClientName = NodeName ++ "_" ++ atom_to_list(DeviceType) ++ "_" ++ integer_to_list(ThisDeviceIndex),
    {ok, ClientPid} = supervisor:start_child(?CHILD_MODULE, [ClientName, DeviceType]),
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
    WildcardBinary = <<"#">>,
    subscribe_clients_to_topic(<<?MQTT_TOPIC_PREFIX/binary, WildcardBinary/binary>>, 0).

subscribe_clients_to_topic(Topic, QoS) ->
    ClientList = persistent_term:get(?MODULE),
    SubscribeFunction = fun({_ClientName, ClientPid}) -> gen_server:call(ClientPid, {subscribe, [{Topic, [{qos, QoS}]}]}) end,
    lists:foreach(SubscribeFunction, ClientList).

start_client_loops() ->
    ClientList = persistent_term:get(?MODULE),
    StartFunction = fun({_ClientName, ClientPid}) -> gen_server:cast(ClientPid, start_client_loops) end,
    lists:foreach(StartFunction, ClientList).

print_clients() ->
    ClientList = persistent_term:get(?MODULE),
    io:format("==== All Clients ====~n", []),
    PrintFunction = fun({ClientName, ClientPid}) -> io:format("~p - ~p~n", [ClientName, ClientPid]) end,
    lists:foreach(PrintFunction, ClientList).