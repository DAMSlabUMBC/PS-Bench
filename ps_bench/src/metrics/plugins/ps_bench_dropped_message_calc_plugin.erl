-module(ps_bench_dropped_message_calc_plugin).

-export([init/1, calc/0]).

init(OutDir) ->
      persistent_term:put({?MODULE, out_dir}, OutDir).

calc() ->
      OverallResults = calculate_overall_dropped_messages(),
      PairwiseResults = calculate_pairwise_dropped_messages(),

      AllResults = [OverallResults] ++ PairwiseResults,
      lists:foreach(fun({SourceNode, DestNode, AllPubsCount, DroppedPubsCount}) -> 
            ps_bench_utils:log_message("Recv by ~p from ~p: Dropped messages - ~p | Total Messages - ~p", [SourceNode, DestNode, DroppedPubsCount, AllPubsCount]) end,
            AllResults).
    
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
      PublishEventCountsByNodeTopic = case TargetNode of
            overall ->
                  ps_bench_store:fetch_mnesia_publish_aggregation();
            _ ->
                  ps_bench_store:fetch_mnesia_publish_aggregation_from_node(TargetNode)
      end,

      ps_bench_utils:log_message("Recvs: ~p", [ps_bench_store:fetch_recv_events()]),
      ps_bench_utils:log_message("Publishes: ~p", [PublishEventCountsByNodeTopic]),


      % At the moment, we make the assumption that every message should be received by every node, which
      % means we have a dropped message if we have less messages recieved than the maximum sequence number sent by the node

      DroppedPubsCount = lists:foldl(fun({_, PubNode, PubTopic, PubMaxSeqId}, TotalDroppedMessages) ->       
                                          % Get all recvs that match this node and topic
                                          RecvEventsFromNodeOnTopic = ps_bench_store:fetch_recv_events_by_filter({ThisNode, '_', PubNode, PubTopic, '_', '_', '_', '_'}),
                                          RecvEventsFromNodeOnTopicCount = length(RecvEventsFromNodeOnTopic),
                                          DroppedPubCount = PubMaxSeqId - RecvEventsFromNodeOnTopicCount,
                                          TotalDroppedMessages + DroppedPubCount
                                          end,
                                          0, PublishEventCountsByNodeTopic),

      AllPubsCount = lists:foldl(fun({_, _, _, PubMaxSeqId}, TotalMessages) ->       
                                          TotalMessages + PubMaxSeqId
                                          end,
                                          0, PublishEventCountsByNodeTopic),
      {ThisNode, TargetNode, AllPubsCount, DroppedPubsCount}.