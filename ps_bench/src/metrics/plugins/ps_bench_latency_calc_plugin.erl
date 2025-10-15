-module(ps_bench_latency_calc_plugin).

-export([init/1, calc/0]).

init(OutDir) ->
      persistent_term:put({?MODULE, out_dir}, OutDir).

calc() ->
      OverallResults = calculate_overall_latency(),
      PairwiseResults = calculate_pairwise_latency(),

      AllResults = [OverallResults] ++ PairwiseResults,
      write_csv(AllResults).
    
calculate_overall_latency() ->
      % Get all messages recieved by this node
      {ok, NodeName} = ps_bench_config_manager:fetch_node_name(),
      calculate_pairwise_latency_for_one_node(NodeName, overall).

calculate_pairwise_latency() ->
      % Get all messages recieved by this node
      {ok, NodeName} = ps_bench_config_manager:fetch_node_name(),
      {ok, AllNodeNames} = ps_bench_config_manager:fetch_node_name_list(),
      lists:map(fun(TargetNode) -> calculate_pairwise_latency_for_one_node(NodeName, TargetNode) end, AllNodeNames).

calculate_pairwise_latency_for_one_node(ThisNode, TargetNode) ->

      AllRecvEvents = case TargetNode of
            overall ->
                  ps_bench_store:fetch_recv_events_by_filter({ThisNode, '_', '_', '_', '_', '_', '_', '_'});
            _ ->
                  ps_bench_store:fetch_recv_events_by_filter({ThisNode, '_', TargetNode, '_', '_', '_', '_', '_'})
      end,

      % Find the overall latency of the entire set of messages
      OverallLatency = lists:foldl(fun(Event, CurrLatencyAcc) -> 
                                                {_, _, _, _, _, TPubNs, TRecvNs, _} = Event,
                                                PacketLatency = TRecvNs - TPubNs,
                                                CurrLatencyAcc + PacketLatency
                                          end, 0, AllRecvEvents),
      TotalMessages = length(AllRecvEvents),

      case TotalMessages of
            0 ->
                  {ThisNode, TargetNode, 0, 0, 0};
            _ ->
                  AvgLatencyNs = OverallLatency / TotalMessages,
                  AvgLatencyMs = AvgLatencyNs / 1000000.0,
                  {ThisNode, TargetNode, OverallLatency, TotalMessages, AvgLatencyMs}
      end.

write_csv(Results) ->
      OutDir = persistent_term:get({?MODULE, out_dir}),
      FullPath = filename:join(OutDir, "latency.csv"),

      % Open file and write the results
      {ok, File} = file:open(FullPath, [write]),
      io:format(File, "Receiver,Sender,SumTotalLatency,TotalMessagesRecv,AverageLatencyMs~n", []),
      lists:foreach(
            fun({SourceNode, DestNode, OverallLatency, TotalMessages, AvgLatencyMs}) ->
                  io:format(File, "~p,~p,~p,~p,~p~n",[SourceNode, DestNode, OverallLatency, TotalMessages, AvgLatencyMs])
            end, Results),
      % Ensure data is written to disk; ignore errors on platforms where sync is not supported
      _ = file:sync(File),
      file:close(File),
      ok.