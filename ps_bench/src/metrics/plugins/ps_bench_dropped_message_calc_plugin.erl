-module(ps_bench_dropped_message_calc_plugin).

-export([init/1, calc/0]).

init(OutDir) ->
      persistent_term:put({?MODULE, out_dir}, OutDir).

calc() ->
      OverallResults = calculate_overall_dropped_messages(),
      PairwiseResults = calculate_pairwise_dropped_messages(),

      AllResults = [OverallResults] ++ PairwiseResults,
      write_csv(AllResults).
    
calculate_overall_dropped_messages() ->
      % Get all messages recieved by this node
      {ok, NodeName} = ps_bench_config_manager:fetch_node_name(),
      calculate_pairwise_dropped_messages_for_one_node(NodeName, overall).

calculate_pairwise_dropped_messages() ->
      % Get all messages recieved by this node
      {ok, NodeName} = ps_bench_config_manager:fetch_node_name(),
      {ok, AllNodeNames} = ps_bench_config_manager:fetch_node_name_list(),
      lists:map(fun(TargetNode) -> calculate_pairwise_dropped_messages_for_one_node(NodeName, TargetNode) end, AllNodeNames).

calculate_pairwise_dropped_messages_for_one_node(ThisNode, TargetNode) ->

      % We also need to match published messages which will tell us if another node sent a message we never recieved
      % (This is a mnesia database and is shared between nodes)
      AllPublishSeqIdsPerTopic = case TargetNode of
            overall ->
                  ps_bench_store:fetch_mnesia_publish_aggregation();
            _ ->
                  ps_bench_store:fetch_mnesia_publish_aggregation_from_node(TargetNode)
      end,

      % Each result is one node
      {FinalDroppedMessages, FinalExpectedMessages} = lists:foldl(fun({_, PubNode, BinaryAggregate}, {TotalDroppedMessages, TotalExpectedMessages}) ->
                                                            TopicMap = erlang:binary_to_term(BinaryAggregate),
                                                            {DroppedMessages, ExpectedMessages} = calculate_dropped_messages_for_node(ThisNode, PubNode, TopicMap),
                                                            {TotalDroppedMessages + DroppedMessages, TotalExpectedMessages + ExpectedMessages}
                                                      end, {0,0}, AllPublishSeqIdsPerTopic),

      {ThisNode, TargetNode, FinalExpectedMessages, FinalDroppedMessages}.

calculate_dropped_messages_for_node(ThisNode, PubNode, TopicMap) ->
      
      TopicList = maps:to_list(TopicMap),

      % Sum the dropped messages for each topic
      lists:foldl(fun({Topic, SeqIdList}, {TotalDroppedMessages, TotalExpectedMessages}) ->
                        {DroppedMessages, ExpectedMessages} = calculate_dropped_message_for_topic(ThisNode, PubNode, Topic, SeqIdList),
                        {TotalDroppedMessages + DroppedMessages, TotalExpectedMessages + ExpectedMessages}
                  end, {0,0}, TopicList).


calculate_dropped_message_for_topic(ThisNode, PubNode, TopicName, ExpectedSeqIds) ->

      % Get a list of all seq ids recv'ed
      RecvEventsFromNodeOnTopic = ps_bench_store:fetch_recv_events_by_filter({ThisNode, '_', PubNode, TopicName, '_', '_', '_', '_'}),
      RecvSeqIds = lists:map(fun({_, _, _, _, SeqId, _, _, _}) -> SeqId end, RecvEventsFromNodeOnTopic),

      % There shouldn't be duplicates but we protect against it to be safe
      FinalExpectedSeqIds = lists:usort(ExpectedSeqIds),
      FinalRecvSeqIds = lists:usort(RecvSeqIds),

      % Find final count
      RemainingIds = lists:subtract(FinalExpectedSeqIds, FinalRecvSeqIds),
      DroppedMessages = length(RemainingIds),
      ExpectedMessages = length(FinalExpectedSeqIds),
      {DroppedMessages, ExpectedMessages}.


write_csv(Results) ->
      OutDir = persistent_term:get({?MODULE, out_dir}),
      FullPath = filename:join(OutDir, "dropped_messages.csv"),

      % Open file and write the results
      {ok, File} = file:open(FullPath, [write]),
      io:format(File, "Receiver,Sender,TotalMessagesSentFromSender,PubsDroppedFromSender,DroppedAsPct~n", []),
      lists:foreach(
            fun({SourceNode, DestNode, AllPubsCount, DroppedPubsCount}) ->
                  PctDropped = case AllPubsCount of
                        0 ->
                              0;
                        _ ->
                              DroppedPubsCount / AllPubsCount
                  end,
                  io:format(File, "~p,~p,~p,~p,~p~n",[SourceNode, DestNode, AllPubsCount, DroppedPubsCount, PctDropped])
            end, Results),
      % Ensure data is written to disk; ignore errors on platforms where sync is not supported
      _ = file:sync(File),
      file:close(File),
      ok.