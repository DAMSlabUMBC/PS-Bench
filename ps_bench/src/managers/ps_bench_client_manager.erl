-module(ps_bench_client_manager).

%% public
-export([start_clients/0, print_clients/0, connect_clients/0,
    subscribe_clients_to_topic/2, publish_data_to_clients/3, disconnect_clients/0]).

start_clients() ->
   
    % Get number of clients and node name
    {ok, ClientCount} = application:get_env(client_count),
    {ok, NodeName} = application:get_env(node_name),

    % Recursively start all the clients, returning the final list
    ClientList = start_clients_recursive(NodeName, ClientCount, []),

    % Store the list of client PIDs using the persistent_term module
    % Reverse the list just to put it in ID order
    persistent_term:put(?MODULE, lists:reverse(ClientList)).


start_clients_recursive(NodeName, NeededClientCount, ClientList) ->

    % Construct name and start the child
    ThisClientCount = length(ClientList) + 1,
    ClientName = NodeName ++ "_Client" ++ integer_to_list(ThisClientCount),
    {ok, ClientPid} = supervisor:start_child(ps_bench_client_sup, [ClientName]),
    NewClientList = [{ClientName, ClientPid} | ClientList],

    if ThisClientCount < NeededClientCount -> 
            start_clients_recursive(NodeName, NeededClientCount, NewClientList);
       ThisClientCount =:= NeededClientCount -> 
            NewClientList
    end.

print_clients() ->
    ClientList = persistent_term:get(?MODULE),
    io:format("==== All Clients ====~n", []),
    PrintFunction = fun({ClientName, ClientPid}) -> io:format("~p - ~p~n", [ClientName, ClientPid]) end,
    lists:foreach(PrintFunction, ClientList).

connect_clients() ->
    ClientList = persistent_term:get(?MODULE),
    ConnectFunction = fun({_ClientName, ClientPid}) -> gen_server:call(ClientPid, connect) end,
    lists:foreach(ConnectFunction, ClientList).

subscribe_clients_to_topic(Topic, QoS) ->
    ClientList = persistent_term:get(?MODULE),
    SubscribeFunction = fun({_ClientName, ClientPid}) -> gen_server:call(ClientPid, {subscribe, #{}, [{Topic, [{qos, QoS}]}]}) end,
    lists:foreach(SubscribeFunction, ClientList).

publish_data_to_clients(Topic, Data, QoS) ->
    ClientList = persistent_term:get(?MODULE),
    PublishFunction = fun({_ClientName, ClientPid}) -> gen_server:call(ClientPid, {publish, #{}, Topic, Data, [{qos, QoS}]}) end,
    lists:foreach(PublishFunction, ClientList).

disconnect_clients() ->
    ClientList = persistent_term:get(?MODULE),
    DisconnectFunction = fun({_ClientName, ClientPid}) -> gen_server:call(ClientPid, disconnect) end,
    lists:foreach(DisconnectFunction, ClientList).