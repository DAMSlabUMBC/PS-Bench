-module(ps_bench_node_manager).
-behaviour(gen_server).

-include("ps_bench_config.hrl").

%% public
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3,
         handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([setup_benchmark/0]).
-export([is_primary_node/0]).

start_link(NodeName) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, NodeName, []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(NodeName) ->
    {ok, #{node_name => NodeName}}.

handle_call(_Msg, _From, State) ->
    {reply, ok, State}.   

handle_cast(local_continue, State = #{node_name := RawNodeName}) ->
    NodeName = ps_bench_utils:convert_to_atom(RawNodeName),
    ps_bench_lifecycle:current_step_complete(NodeName),
    {noreply, State};

handle_cast(global_continue, State = #{node_name := RawNodeName}) ->
    NodeName = ps_bench_utils:convert_to_atom(RawNodeName),
    {ok, Nodes} = ps_bench_config_manager:fetch_node_list(),
    rpc:multicall(Nodes, ps_bench_lifecycle, current_step_complete, [NodeName]),
    {noreply, State};

handle_cast(_Other, State) ->
    {noreply, State}.

handle_info({Pid, Command}, State) ->
    % Make sure pid is of this node's lifecycle fsm
    LifecyclePid = whereis(ps_bench_lifecycle),
    case Pid =:= LifecyclePid of
        true -> 
            case handle_next_step_command(Command) of
                ok -> {noreply, State};
                {error, Reason} -> {stop, Reason}
            end;
        false -> {noreply, State}
    end.

handle_next_step_command(start_connections) ->
    ps_bench_utils:log_state_change("Establishing Connections"),

    % Find all other nodes
    {ok, NodeList} = ps_bench_config_manager:fetch_node_list(),
    CurrentNode = node(),
    OtherNodes = lists:delete(CurrentNode, NodeList),
    case wait_for_nodes_to_connect(OtherNodes) of
        ok ->
            ps_bench_store:initialize_mnesia_storage(NodeList),
            gen_server:cast(?MODULE, global_continue);
        {error, Reason} ->
            {error, Reason}
    end;

handle_next_step_command(start_initialization) ->
    ps_bench_utils:log_state_change("Initializing Benchmark Node"),

    % Initialize random number generator
    initialize_rng_seed(),

    % Create storage tables
    ok = ps_bench_store:initialize_node_storage(),

    % Now initalize the scenario
    ok = ps_bench_scenario_manager:initialize_scenario(),

    % Finally initialize the metric plugins
    ps_bench_metrics_manager:initialize_plugins(),

    % Ready to start when other nodes are synced
    gen_server:cast(?MODULE, global_continue),
    ok;


handle_next_step_command(start_benchmark) ->

    % Start hardware polling if we're using it
    case ps_bench_config_manager:using_hw_poll() of
        true ->
            gen_server:call(ps_bench_metrics_hw_stats_reader, start_polling);
        false ->
            ok
    end,

    % Start the scenario
    ps_bench_scenario_manager:run_scenario(),
    ok;

handle_next_step_command(finalize_scenario) ->

    % Stop hardware polling if we're using it
    case ps_bench_config_manager:using_hw_poll() of
        true ->
            gen_server:call(ps_bench_metrics_hw_stats_reader, stop_polling);
        false ->
            ok
    end,

    ps_bench_scenario_manager:clean_up_scenario(),
    ps_bench_store:store_aggregate_publish_results_in_mnesia(),
    gen_server:cast(?MODULE, global_continue),
    ok;

handle_next_step_command(start_calculate_metrics) ->
    ps_bench_utils:log_state_change("Starting Metric Calc"),
    
    % Wait for final messages
    timer:sleep(2000),

    ps_bench_metrics_manager:run_metric_calculations(),
    
    % Write local metrics first
    % ps_bench_metrics_rollup:write_csv(),
    
    % % If primary node, aggregate from all nodes
    % case is_primary_node() of
    %     true ->
    %         ps_bench_utils:log_message("Primary node: aggregating metrics from all nodes"),
    %         ps_bench_metrics_aggregator:aggregate_metrics();
    %     false ->
    %         ps_bench_utils:log_message("Secondary node: skipping aggregation"),
    %         ok
    % end,
    
    gen_server:cast(?MODULE, global_continue),
    ok;

handle_next_step_command(start_clean_up) ->
    ps_bench_utils:log_state_change("Starting Cleanup"),
    % ps_bench_metrics_rollup:write_csv(),
    ps_bench_app:stop_benchmark_application(),
    ok.

terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> 
    {ok, State}.

is_primary_node() ->
    % We call ourselves primary if we're the first in the scenario config list
    {ok, [PrimaryNode | _]} = ps_bench_config_manager:fetch_node_list(),
    (PrimaryNode =:= node()).

%%%===================================================================
%%% Lifecycle Managment calls
%%%===================================================================
setup_benchmark() ->

    % Register this node
    {ok, NodeName} = ps_bench_config_manager:fetch_node_name(),
    _ = ensure_distribution(NodeName),
    erlang:set_cookie(node(), ?PS_BENCH_COOKIE),

    % At this point in the call, we're done configuring, so let the lifecycle manager know
    gen_server:cast(?MODULE, local_continue).

ensure_distribution(NodeName0) ->
    case node() of
        nonode@nohost ->
            NodeName = ps_bench_utils:convert_to_atom(NodeName0),
            DistMode = case os:getenv("ERLANG_DIST_MODE") of
                "longnames" -> longnames;
                "name" -> longnames;
                _ -> shortnames  % default to shortnames
            end,
            case net_kernel:start([NodeName, DistMode]) of
                {ok, _Pid}                       -> ok;
                {error, {already_started, _Pid}} -> ok;
                {error, Reason} ->
                    ps_bench_utils:log_message(
                      "WARNING: distribution not started (~p). Running local-only.", [Reason]),
                    ok
            end;
        _DistributedName ->
            ok
    end.

wait_for_nodes_to_connect([]) ->
    ok;

wait_for_nodes_to_connect([NextNode | OtherNodes]) ->
    ps_bench_utils:log_message("Attempting to connect to ~p", [NextNode]),
    case wait_for_node_to_connect(NextNode, 30) of
        timeout ->
            Reason = io_lib:format("Failed to connect to node ~s", [NextNode]),
            {error, Reason};
        ok ->
            ps_bench_utils:log_message("Connected to ~p", [NextNode]),
            wait_for_nodes_to_connect(OtherNodes)
    end.

wait_for_node_to_connect(Node, RetryCount) ->
    case net_kernel:connect_node(Node) of
        true ->
            ok;
        _ ->
            if RetryCount =< 0 ->
                timeout;
            RetryCount > 0 ->
                % Wait 1s and try again
                receive
                after (1000) -> wait_for_node_to_connect(Node, RetryCount - 1)
                end
            end
    end.

initialize_rng_seed() ->
    % Need to check if we have one in config or if we need to make a new one
    {ok, NodeName} = ps_bench_config_manager:fetch_node_name(),
    case ps_bench_config_manager:fetch_rng_seed_for_node(NodeName) of
        {ok, Seed} ->
            % Use configured seed
            ps_bench_utils:initialize_rng_seed(Seed);
        {error, unknown_property} ->
            % Generate a new seed
            ps_bench_utils:initialize_rng_seed()
    end.