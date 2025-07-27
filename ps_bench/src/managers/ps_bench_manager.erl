-module(ps_bench_manager).
-behaviour(gen_server).

%% public
-export([start_link/1]).

-export([start_next_step/2]).

%% gen_server callbacks
-export([init/1, handle_call/3,
         handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

start_link(NodeName) ->
    gen_server:start_link({local,?MODULE}, ?MODULE, NodeName, []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(NodeName) ->
    {ok, #{node_name => NodeName}}.

handle_call(_Msg, _From, State) ->
    %% no call logic yet, just fulfill behaviour
    {noreply, State}.

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
        true -> start_next_step(Command, State);
        false -> {noreply, State}
    end.

start_next_step(start_benchmark, State = #{node_name := NodeName}) ->
    io:format("~s Starting Benchmark~n", [NodeName]),
    ps_bench_lifecycle:current_step_complete(NodeName),
    {noreply, State};

start_next_step(start_calculate_metrics, State = #{node_name := NodeName}) ->
    io:format("~s Starting Metric Calc~n", [NodeName]),
    ps_bench_lifecycle:current_step_complete(NodeName),
    {noreply, State};

start_next_step(start_clean_up, State = #{node_name := NodeName}) ->
    io:format("~s Starting Cleanup~n", [NodeName]),
    {noreply, State}.

terminate(normal, {TestName, ClientName}) -> 
    io:format("Manager shutdown ~s for ~s~n",[TestName, ClientName]),
    ok;

terminate(shutdown, {TestName, ClientName}) ->
    io:format("Manager shutdown ~s for ~s~n",[TestName, ClientName]),
    ok.

code_change(_OldVsn, State, _Extra) -> 
    {ok, State}.