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

      AllRecvEvents = case TargetNode of
            overall ->
                  ps_bench_store:fetch_recv_events_by_filter({ThisNode, '_', '_', '_', '_', '_', '_', '_'});
            _ ->
                  ps_bench_store:fetch_recv_events_by_filter({ThisNode, '_', TargetNode, '_', '_', '_', '_', '_'})
      end,

      % We also need to match published messages which will tell us if another node sent a message we never recieved
      % (This is a mnesia database and is shared between nodes)
      AllPublishEvents = case TargetNode of
            overall ->
                  ps_bench_store:fetch_publish_events();
            _ ->
                  ps_bench_store:fetch_publish_events_from_node(TargetNode)
      end,

      % At the moment, we make the assumption that every message should be received by every node, which
      % means we have a dropped message if this node did not recieve a message in the publish event list
      DroppedPubs = lists:filter(fun(Event) -> not does_recv_exist_for_publish(Event, AllRecvEvents) end, AllPublishEvents),
      DroppedPubsCount = length(DroppedPubs),
      AllPubsCount = length(AllPublishEvents),
      {ThisNode, TargetNode, AllPubsCount, DroppedPubsCount}.

does_recv_exist_for_publish({_, SendingNode, _, PubTopic, PubSeqId}, AllRecvEvents) ->
      % Check if a recv message matches this pub
      RecvEventForPub = lists:filter(fun(Event) ->
                              {_, _, RecvFromNode, RecvTopic, RecvSeqID, _, _, _} = Event,
                              if    RecvFromNode =:= SendingNode, PubTopic =:= RecvTopic, PubSeqId =:= RecvSeqID -> true;
                                    true -> false
                              end end, AllRecvEvents),

      case RecvEventForPub of
            [] -> false;
            _ -> true
      end.