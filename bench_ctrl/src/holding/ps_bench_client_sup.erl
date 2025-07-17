-module(ps_bench_client_sup).
-behaviour(supervisor).

-export([start_clients/0]).

-export([start_link/1, init/1]).

-define(WORKER_MODULE, ps_bench_client).

start_link(TestName) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, TestName).

init(TestName) ->
    Template = #{id => client_worker,
                 start => {?WORKER_MODULE, start_link, [TestName]},
                 restart => transient, 
                 shutdown => 5000,
                 type => worker, 
                 modules => [?WORKER_MODULE]},
    % TODO: Tune or allow configuration of the timing and restart parameters
    {ok, {{simple_one_for_one, 10, 60}, [Template]}}.

start_clients() ->
   
    % Get number of clients and node name
    {ok, ClientCount} = application:get_env(client_interface_name),
    {ok, NodeName} = application:get_env(node_name),

    % Recursively start all the clients
    start_client(NodeName, ClientCount, 0).


start_client(NodeName, TotalClients, CurrentClients) ->

    % Construct name and start the child
    ClientName = NodeName ++ "_" ++ CurrentClients,
    supervisor:start_child(?MODULE, [ClientName]),

    if TotalClients < CurrentClients ->
        start_client(NodeName, TotalClients, CurrentClients + 1)
    end.


