-module(ps_bench_lifecycle).
-behaviour(gen_statem).

% Process managment callbacks
-export([start_link/2,current_step_complete/1]).

% Callbacks for gen_statem
-export([init/1,callback_mode/0]).

% States
-export([configuring/3,connecting/3,initializing/3,benchmarking/3,finalizing/3,calculating_metrics/3,done/3]).

start_link(NodeList, SetupTimeout) ->
    NodeStatusMap = #{all_nodes => NodeList, pending_nodes => NodeList, timeout => SetupTimeout},
    gen_statem:start_link({local,?MODULE}, ?MODULE, NodeStatusMap, []).

init(NodeStatusMap) ->
    {ok, configuring, NodeStatusMap}.

callback_mode() ->
    [state_functions, state_enter].

% This should be called whenever the benchmarking manager on a node
% is ready to transition to the next phase of the application
current_step_complete(NodeName) ->
    gen_statem:cast(?MODULE, NodeName).

% ============== State machine definitions ==============
% When entering the initial state, set a timeout to ensure we don't block forever
configuring(enter, _OldState, #{timeout := Timeout}) ->
    {keep_state_and_data, 
        {state_timeout,Timeout,setup}};

% Exit the state machine if timeout occured
configuring(state_timeout, setup, _) ->
    {stop, "Benchmark nodes did not initialize within the configured time"};

configuring(cast, _NodeName, Data) ->
    % We don't have to wait for other nodes for this step, just move to the next step
    {next_state, connecting, Data}.

% Instruct the manager to start connecting to other known nodes
connecting(enter, _OldState, _State) ->
    ManagerPid = whereis(ps_bench_node_manager),
    ManagerPid ! {self(), start_connections},
    keep_state_and_data;

connecting(cast, NodeName, #{all_nodes := AllNodes, pending_nodes := PendingNodes} = Data) ->
    evaluate_continuation(NodeName, AllNodes, PendingNodes, Data, initializing).

% Instruct the manager to initialize the tests
initializing(enter, _OldState, _State) ->
    ManagerPid = whereis(ps_bench_node_manager),
    ManagerPid ! {self(), start_initialization},
    keep_state_and_data;

% Process casts until every node is ready to transition state
initializing(cast, NodeName, #{all_nodes := AllNodes, pending_nodes := PendingNodes} = Data) ->
    evaluate_continuation(NodeName, AllNodes, PendingNodes, Data, benchmarking).

% Instruct the manager to start benchmarking
benchmarking(enter, _OldState, _State) ->
    ManagerPid = whereis(ps_bench_node_manager),
    ManagerPid ! {self(), start_benchmark},
    keep_state_and_data;

% Process casts until every node is ready to transition state
benchmarking(cast, NodeName, #{all_nodes := AllNodes, pending_nodes := PendingNodes} = Data) ->
    evaluate_continuation(NodeName, AllNodes, PendingNodes, Data, finalizing).

% Instruct the manager to clean up the benchmarking objects
finalizing(enter, _OldState, _State) ->
    ManagerPid = whereis(ps_bench_node_manager),
    ManagerPid ! {self(), finalize_scenario},
    keep_state_and_data;

% Process casts until every node is ready to transition state
finalizing(cast, NodeName, #{all_nodes := AllNodes, pending_nodes := PendingNodes} = Data) ->
    evaluate_continuation(NodeName, AllNodes, PendingNodes, Data, calculating_metrics).

% Instruct the manager to start metric calculation
calculating_metrics(enter, _OldState, _State) ->
    ManagerPid = whereis(ps_bench_node_manager),
    ManagerPid ! {self(), start_calculate_metrics},
    keep_state_and_data;

% Process casts until every node is ready to transition state
calculating_metrics(cast, NodeName, #{all_nodes := AllNodes, pending_nodes := PendingNodes} = Data) ->
    evaluate_continuation(NodeName, AllNodes, PendingNodes, Data, done).

% The only thing we do in this state is instruct the manager to shutdown the benchmark
done(enter, _OldState, _State) ->
    ManagerPid = whereis(ps_bench_node_manager),
    ManagerPid ! {self(), start_clean_up},
    keep_state_and_data;

% We don't want to crash if we get repeated messages here
done(cast, _, _) ->
    keep_state_and_data.

evaluate_continuation(ThisNode, AllNodes, PendingNodes, Data, NextStep) ->
    NewPendingNodes = PendingNodes -- [ThisNode],
    case NewPendingNodes of
        [] -> 
            {next_state, NextStep, Data#{pending_nodes := AllNodes}};
        _ -> 
            {keep_state, Data#{pending_nodes := NewPendingNodes}}
    end.