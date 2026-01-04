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

      TotalMessages = length(AllRecvEvents),

      case TotalMessages of
            0 ->
                  {ThisNode, TargetNode, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
            _ ->
                  % Find the overall latency of the entire set of messages
                  OverallLatency = lists:foldl(fun(Event, CurrLatencyAcc) -> 
                                                            {_, _, _, _, _, TPubNs, TRecvNs, _} = Event,
                                                            PacketLatency = TRecvNs - TPubNs,
                                                            CurrLatencyAcc + PacketLatency
                                                      end, 0, AllRecvEvents),

                  AvgLatencyNs = OverallLatency / TotalMessages,
                  AvgLatencyMs = AvgLatencyNs / 1000000.0,

                  % Now compute sum of squared means
                  SumOfSquaredMeans = lists:foldl(fun(Event, CurrSum) -> 
                                                            {_, _, _, _, _, TPubNs, TRecvNs, _} = Event,
                                                            PacketLatency = TRecvNs - TPubNs,
                                                            Difference = PacketLatency - AvgLatencyNs,
                                                            SquaredDifference = math:pow(Difference, 2),
                                                            CurrSum + SquaredDifference
                                                      end, 0, AllRecvEvents),
                  VarianceNs = SumOfSquaredMeans / TotalMessages,
                  VarianceMs = VarianceNs / 1000000.0,

                  % Now compute the median value
                  SortedLatencies = lists:sort(fun(Event1, Event2) ->
                                                      {_, _, _, _, _, TPubNs1, TRecvNs1, _} = Event1,
                                                      {_, _, _, _, _, TPubNs2, TRecvNs2, _} = Event2,
                                                      PacketLatency1 = TRecvNs1 - TPubNs1,
                                                      PacketLatency2 = TRecvNs2 - TPubNs2,
                                                      PacketLatency1 < PacketLatency2
                                                end, AllRecvEvents),
                  MedianIndex = TotalMessages div 2, % Integer divison
                  {_, _, _, _, _, MedianTPubNs, MedianTRecvNs, _} = lists:nth(MedianIndex, SortedLatencies),
                  MedianValue = MedianTRecvNs - MedianTPubNs,
                  MedianValueMs = MedianValue / 1000000.0,

                  % Now compute P90/95/99 values with the sorted latencies
                  % First calculate indexes as (TotalMessages * p_val)
                  % e.g. for 500 messages, the 90th percentile index would be 450
                  % We round down for a more conservative measurement
                  P90Index = erlang:trunc(TotalMessages * 0.90),
                  P95Index = erlang:trunc(TotalMessages * 0.95),
                  P99Index = erlang:trunc(TotalMessages * 0.99),

                  {_, _, _, _, _, P90TPubNs, P90TRecvNs, _} = lists:nth(P90Index, SortedLatencies),
                  P90ValueNs = P90TRecvNs - P90TPubNs,
                  P90ValueMs = P90ValueNs / 1000000.0,

                  {_, _, _, _, _, P95TPubNs, P95TRecvNs, _} = lists:nth(P95Index, SortedLatencies),
                  P95ValueNs = P95TRecvNs - P95TPubNs,
                  P95ValueMs = P95ValueNs / 1000000.0,

                  {_, _, _, _, _, P99TPubNs, P99TRecvNs, _} = lists:nth(P99Index, SortedLatencies),
                  P99ValueNs = P99TRecvNs - P99TPubNs,
                  P99ValueMs = P99ValueNs / 1000000.0,

                  % Min/Max
                  {_, _, _, _, _, MinTPubNs, MinTRecvNs, _} = lists:nth(1, SortedLatencies),
                  MinValueNs = MinTRecvNs - MinTPubNs,
                  MinValueMs = MinValueNs / 1000000.0,

                  {_, _, _, _, _, MaxTPubNs, MaxTRecvNs, _} = lists:nth(1, lists:reverse(SortedLatencies)),
                  MaxValueNs = MaxTRecvNs - MaxTPubNs,
                  MaxValueMs = MaxValueNs / 1000000.0,

                  {ThisNode, TargetNode, OverallLatency, TotalMessages, AvgLatencyMs, VarianceMs, MinValueMs, MaxValueMs, MedianValueMs, P90ValueMs, P95ValueMs, P99ValueMs}
      end.

write_csv(Results) ->
      OutDir = persistent_term:get({?MODULE, out_dir}),
      FullPath = filename:join(OutDir, "latency.csv"),

      % Open file and write the results
      {ok, File} = file:open(FullPath, [write]),
      io:format(File, "Receiver,Sender,SumTotalLatency,TotalMessagesRecv,AverageLatencyMs,VarianceMs,MinMs,MaxMs,MedianMs,P90Ms,P95Ms,P99Ms~n", []),
      lists:foreach(
            fun({SourceNode, DestNode, OverallLatency, TotalMessages, AvgLatencyMs, VarianceMs, MinValueMs, MaxValueMs, MedianValueMs, P90ValueMs, P95ValueMs, P99ValueMs}) ->
                  io:format(File, "~p,~p,~p,~p,~p,~p,~p,~p,~p,~p,~p,~p~n",[SourceNode, DestNode, OverallLatency, TotalMessages, AvgLatencyMs, VarianceMs, MinValueMs, MaxValueMs, MedianValueMs, P90ValueMs, P95ValueMs, P99ValueMs])
            end, Results),
      % Ensure data is written to disk; ignore errors on platforms where sync is not supported
      _ = file:sync(File),
      file:close(File),
      ok.