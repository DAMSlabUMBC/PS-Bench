-module(ps_bench_lifecycle).
-behaviour(gen_statem).

% Process managment callbacks
-export([start_link/2,current_step_complete/1]).

% Callbacks for gen_statem
-export([init/1,callback_mode/0]).

% States
-export([configuring/3,benchmarking/3,calculating_metrics/3,done/3]).

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

% Process casts until every node is ready to transition state
configuring(cast, NodeName, #{all_nodes := AllNodes, pending_nodes := PendingNodes} = Data) ->
    NewPendingNodes = PendingNodes -- [NodeName],
    case NewPendingNodes of
        [] -> 
            {next_state, benchmarking, Data#{pending_nodes := AllNodes}};
        _ -> 
            {keep_state, Data#{pending_nodes := NewPendingNodes}}
        end.

% Instruct the manager to start benchmarking
benchmarking(enter, _OldState, _State) ->
    ManagerPid = whereis(ps_bench_node_manager),
    ManagerPid ! {self(), start_benchmark},
    keep_state_and_data;

% Process casts until every node is ready to transition state
benchmarking(cast, NodeName, #{all_nodes := AllNodes, pending_nodes := PendingNodes} = Data) ->
    NewPendingNodes = PendingNodes -- [NodeName],
    case NewPendingNodes of
        [] -> 
            {next_state, calculating_metrics, Data#{pending_nodes := AllNodes}};
        _ -> 
            {keep_state, Data#{pending_nodes := NewPendingNodes}}
    end.

% Instruct the manager to start metric calculation
calculating_metrics(enter, _OldState, _State) ->
    ManagerPid = whereis(ps_bench_node_manager),
    ManagerPid ! {self(), start_calculate_metrics},
    keep_state_and_data;

% Process casts until every node is ready to transition state
calculating_metrics(cast, NodeName, #{all_nodes := AllNodes, pending_nodes := PendingNodes} = Data) ->
    NewPendingNodes = PendingNodes -- [NodeName],
    case NewPendingNodes of
        [] -> 
            {next_state, done, Data#{pending_nodes := AllNodes}};
        _ -> 
            {keep_state, Data#{pending_nodes := NewPendingNodes}}
        end.

% The only thing we do in this state is instruct the manager to shutdown the benchmark
done(enter, _OldState, _State) ->
    ManagerPid = whereis(ps_bench_node_manager),
    ManagerPid ! {self(), start_clean_up},
    keep_state_and_data;

% We don't want to crash if we get repeated messages here
done(cast, _, _) ->
    keep_state_and_data.