-module(ps_bench_node_manager).
-behaviour(gen_server).

%% public
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3,
         handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([initialize_benchmark/0]).

start_link(NodeName) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, NodeName, []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(NodeName) ->
    {ok, #{node_name => NodeName}}.

handle_call(_Msg, _From, State) ->
    {reply, ok, State}.   

handle_cast(local_continue, State = #{node_name := NodeName}) ->
    ps_bench_lifecycle:current_step_complete(NodeName),
    {noreply, State};

handle_cast({remote_continue, RemoteNodeName}, State) ->
    ps_bench_lifecycle:current_step_complete(RemoteNodeName),
    {noreply, State}.

handle_info({Pid, Command}, State) ->
    % Make sure pid is of this node's lifecycle fsm
    LifecyclePid = whereis(ps_bench_lifecycle),
    case Pid =:= LifecyclePid of
        true -> 
            handle_next_step_command(Command),
            {noreply, State};
        false -> {noreply, State}
    end.

handle_next_step_command(start_benchmark) ->
    ps_bench_scenario_manager:run_scenario();

handle_next_step_command(start_calculate_metrics) ->
    ps_bench_utils:log_state_change("Starting Metric Calc"),
    gen_server:cast(?MODULE, local_continue);

handle_next_step_command(start_clean_up) ->
    ps_bench_utils:log_state_change("Starting Cleanup"),
    ps_bench_app:stop_benchmark_application().

terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> 
    {ok, State}.

%%%===================================================================
%%% Lifecycle Managment calls
%%%===================================================================
initialize_benchmark() ->

    % Initialize random number generator
    ps_bench_utils:initialize_rng_seed(), % TODO, need to sync across all nodes and allow loading from config

    % Now initalize the scenario
    ps_bench_scenario_manager:initialize_scenario(),

    % At this point in the call, we're done configuring, so let the lifecycle manager know
    gen_server:cast(?MODULE, local_continue).
    