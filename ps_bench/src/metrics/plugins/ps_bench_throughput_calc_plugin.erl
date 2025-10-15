-module(ps_bench_throughput_calc_plugin).

-export([init/1, calc/0]).

init(OutDir) ->
      persistent_term:put({?MODULE, out_dir}, OutDir).

calc() ->
      OverallResults = calculate_overall_throughput(),
      PairwiseResults = calculate_pairwise_throughput(),

      AllResults = [OverallResults] ++ PairwiseResults,
      write_csv(AllResults).
    
calculate_overall_throughput() ->
      % Get all messages recieved by this node
      {ok, NodeName} = ps_bench_config_manager:fetch_node_name(),
      calculate_pairwise_throughput_for_one_node(NodeName, overall).

calculate_pairwise_throughput() ->
      % Get all messages recieved by this node
      {ok, NodeName} = ps_bench_config_manager:fetch_node_name(),
      {ok, AllNodeNames} = ps_bench_config_manager:fetch_node_name_list(),
      lists:map(fun(TargetNode) -> calculate_pairwise_throughput_for_one_node(NodeName, TargetNode) end, AllNodeNames).

calculate_pairwise_throughput_for_one_node(ThisNode, TargetNode) ->

      AllRecvEvents = case TargetNode of
            overall ->
                  ps_bench_store:fetch_recv_events_by_filter({ThisNode, '_', '_', '_', '_', '_', '_', '_'});
            _ ->
                  ps_bench_store:fetch_recv_events_by_filter({ThisNode, '_', TargetNode, '_', '_', '_', '_', '_'})
      end,

      TotalMessages = length(AllRecvEvents),

      case TotalMessages of
            0 ->
                  {ThisNode, TargetNode, 0, 0, 0};
            _ ->
                 % Bootstrap the calculation by setting min/max equal to the first element of the list
                  [FirstElement | _] = AllRecvEvents,
                  {_, _, _, _, _, _, InitialTRecvNs, _} = FirstElement,

                  % Find the min and max time this node recv to calc duration
                  {OverallMinTimeNs, OverallMaxTimNs} = lists:foldl(fun(Event, {CurrMinTime, CurrMaxTime}) -> 
                                                            {_, _, _, _, _, _, TRecvNs, _} = Event,
                                                            NewMinTime = min(TRecvNs, CurrMinTime),
                                                            NewMaxTime = max(TRecvNs, CurrMaxTime),
                                                            {NewMinTime, NewMaxTime}
                                                      end, {InitialTRecvNs, InitialTRecvNs}, AllRecvEvents),
                  

                  DurationS = (OverallMaxTimNs - OverallMinTimeNs) / 1000000000.0,
                  Throughput = TotalMessages / DurationS,

                  % Now partition recieved messages based on seconds
                  PartitionedList = partition_throughput_list(AllRecvEvents, OverallMinTimeNs, OverallMaxTimNs, []),

                  % Now compute sum of squared means
                  SumOfSquaredMeans = lists:foldl(fun(InnerList, CurrSum) -> 
                                                            MessagesInWindow = length(InnerList),
                                                            Difference = MessagesInWindow - Throughput,
                                                            SquaredDifference = math:pow(Difference, 2),
                                                            CurrSum + SquaredDifference
                                                      end, 0, PartitionedList),
                  Variance = SumOfSquaredMeans / DurationS,

                  {ThisNode, TargetNode, DurationS, TotalMessages, Throughput, Variance}
      end.

partition_throughput_list(List, CurrTime, EndTime, Acc) ->

      EndTimeForPartition = min(CurrTime + 1000000000, EndTime),
      MessagesInTime = lists:filter(fun(Event) -> 
                                          {_, _, _, _, _, _, TRecvNs, _} = Event,
                                          TRecvNs =< EndTimeForPartition andalso TRecvNs >= CurrTime
                                          end, List),
      NewAcc = Acc ++ [MessagesInTime],

      case EndTimeForPartition of 
            Value when EndTimeForPartition =/= EndTime ->
                  partition_throughput_list(List, CurrTime + 1000000000, EndTime, NewAcc);
            _ ->
                  NewAcc
      end.


write_csv(Results) -> 
      OutDir = persistent_term:get({?MODULE, out_dir}),
      FullPath = filename:join(OutDir, "throughput.csv"),

      % Open file and write the results
      {ok, File} = file:open(FullPath, [write]),
      io:format(File, "Receiver,Sender,DurationSeconds,TotalMessagesRecv,AverageThroughput,Variance~n", []),
      lists:foreach(
            fun({SourceNode, DestNode, DurationS, TotalMessages, Throughput, Variance}) ->
                  io:format(File, "~p,~p,~p,~p,~p,~p~n",[SourceNode, DestNode, DurationS, TotalMessages, Throughput, Variance])
            end, Results),
      % Ensure data is written to disk; ignore errors on platforms where sync is not supported
      _ = file:sync(File),
      file:close(File), 
      ok.