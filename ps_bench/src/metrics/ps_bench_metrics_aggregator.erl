-module(ps_bench_metrics_aggregator).
-behaviour(gen_server).

-export([start_link/0, aggregate_metrics/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    {ok, #{}}.

aggregate_metrics() ->
    gen_server:call(?MODULE, aggregate, 60000).  % 60s timeout

handle_call(aggregate, _From, State) ->
    ps_bench_utils:log_message("Starting multi-node metric aggregation"),
    {ok, NodeList} = ps_bench_config_manager:fetch_node_list(),
    
    % Collect from all nodes
    Results = lists:map(
        fun(Node) ->
            ps_bench_utils:log_message("Collecting metrics from node: ~p", [Node]),
            case rpc:call(Node, ps_bench_store, list_window_summaries, [], 5000) of
                {badrpc, Reason} -> 
                    ps_bench_utils:log_message("Failed to collect from ~p: ~p", [Node, Reason]),
                    [];
                Summaries -> 
                    ps_bench_utils:log_message("Got ~p summaries from ~p", [length(Summaries), Node]),
                    Summaries
            end
        end, NodeList),
    
    % Merge results
    MergedResults = merge_summaries(lists:flatten(Results)),
    
    % Write to file
    write_aggregated_csv(MergedResults),
    
    {reply, ok, State};

handle_call(_, _From, State) ->
    {reply, ok, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info(_, State) ->
    {noreply, State}.

terminate(_, _) ->
    ok.

code_change(_, State, _) ->
    {ok, State}.

%% Internal functions
merge_summaries(AllSummaries) ->
    % Group by window
    Grouped = lists:foldl(
        fun({{RunId, WinMs}, Summary}, Acc) ->
            Key = {RunId, WinMs},
            case maps:find(Key, Acc) of
                {ok, Existing} ->
                    Merged = #{
                        msgs => maps:get(msgs, Existing, 0) + maps:get(msgs, Summary, 0),
                        bytes => maps:get(bytes, Existing, 0) + maps:get(bytes, Summary, 0),
                        drops => maps:get(drops, Existing, 0) + maps:get(drops, Summary, 0),
                        msgs_per_s => maps:get(msgs_per_s, Existing, 0) + maps:get(msgs_per_s, Summary, 0)
                    },
                    maps:put(Key, Merged, Acc);
                error ->
                    maps:put(Key, Summary, Acc)
            end
        end, #{}, AllSummaries),
    maps:to_list(Grouped).

write_aggregated_csv(MergedResults) ->
    {ok, BasePath} = ps_bench_config_manager:fetch_metrics_output_dir(),
    Path = filename:join(BasePath, "aggregated_metrics.csv"),
    ps_bench_utils:log_message("Writing aggregated metrics to: ~s", [Path]),
    {ok, F} = file:open(Path, [write]),
    io:format(F, "run_id,win_start_ms,total_msgs,total_bytes,total_msgs_per_s,total_drops~n", []),
    lists:foreach(
        fun({{RunId, WinMs}, Summary}) ->
            io:format(F, "~p,~B,~B,~B,~B,~B~n",
                [RunId, WinMs,
                 maps:get(msgs, Summary, 0),
                 maps:get(bytes, Summary, 0),
                 maps:get(msgs_per_s, Summary, 0),
                 maps:get(drops, Summary, 0)])
        end, lists:sort(MergedResults)),
    file:close(F),
    ps_bench_utils:log_message("Aggregated metrics written successfully"),
    ok.