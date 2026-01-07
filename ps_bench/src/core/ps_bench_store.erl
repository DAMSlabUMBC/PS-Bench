-module(ps_bench_store).

-include("ps_bench_config.hrl").

-export([initialize_node_storage/0, initialize_mnesia_storage/1]).
%% seq mgmt
-export([get_next_seq_id/1]).
%% recv event I/O
-export([record_recv/6, record_recv/7, record_publish/3, record_connect/2, record_disconnect/3, store_aggregate_publish_results_in_mnesia/0, record_cpu_usage/3, record_memory_usage/3]).
%% rollup helpers
-export([get_last_recv_seq/1, put_last_recv_seq/2]).
%% window summaries
-export([fetch_recv_events/0, fetch_recv_events_by_filter/1, fetch_publish_events/0, fetch_publish_events_by_filter/1, fetch_connect_events/0, fetch_disconnect_events/0]).
%% write to disk
-export([write_publish_events_to_disk/1, write_recv_events_to_disk/1, write_connect_events_to_disk/1, write_disconnect_events_to_disk/1, write_cpu_usage_events_to_disk/1, write_memory_usage_events_to_disk/1]).
-export([fetch_mnesia_publish_aggregation/0, fetch_mnesia_publish_aggregation_from_node/1, fetch_cpu_usage/0, fetch_broker_cpu_usage/0, fetch_memory_usage/0, fetch_broker_memory_usage/0]).


-define(T_PUBSEQ, psb_pub_seq).      %% {pub_topic, TopicBin} -> Seq
-define(T_RECVSEQ, psb_recv_seq).    %% {recv_topic, TopicBin} -> LastSeqSeen
-define(T_CONNECT_EVENTS, psb_connect_events).
-define(T_DISCONNECT_EVENTS, psb_disconnect_events).
-define(T_PUBLISH_EVENTS, psb_publish_events).       %% ordered by t_recv_ns key
-define(T_RECV_EVENTS, psb_recv_events).
-define(T_CPU_EVENTS, cpu_usage_events).
-define(T_MEM_EVENTS, mem_usage_events).

initialize_node_storage() ->
    % Clear any existing tables first
    lists:foreach(fun(Table) ->
        catch ets:delete(Table)
    end, [?T_PUBSEQ, ?T_RECVSEQ, ?T_CONNECT_EVENTS, 
          ?T_DISCONNECT_EVENTS, ?T_PUBLISH_EVENTS, 
          ?T_RECV_EVENTS]),
    
    % Now create fresh tables
    ensure_tables(),
    ok.

%% publisher seq generation (per-topic) 
get_next_seq_id(Topic) ->
    ensure_tables(),
    Key = {pub_topic, Topic},
    ets:update_counter(?T_PUBSEQ, Key, {2,1}, {Key,0}).

%% Create mnesia schema and start mnesia
initialize_mnesia_storage(Nodes) ->
    case ps_bench_node_manager:is_primary_node() of
        true ->
            ps_bench_utils:log_message("Initializing mnesia schema on ~p. You may see an exit call for mnesia, this is fine.", [Nodes]),
            rpc:multicall(Nodes, application, stop, [mnesia]),
            mnesia:create_schema(Nodes),
            ps_bench_utils:log_message("Attempting to restart mnesia database on ~p.", [Nodes]),
            {_Results, _BadNodes} = rpc:multicall(Nodes, application, start, [mnesia]),
            
            % Wait for all nodes to report mnesia is started
            wait_for_mnesia_on_nodes(Nodes),
            
            % Add schema copies and ensure all nodes know about each other
            % lists:foreach(fun(Node) -> 
            %     mnesia:add_table_copy(schema, Node, ram_copies) 
            % end, Nodes -- [node()]),
            
            % This is the critical fix - ensure ALL nodes know about each other
            % rpc:multicall(Nodes, mnesia, change_config, [extra_db_nodes, Nodes]),
            
            ps_bench_utils:log_message("Creating mnesia tables on ~p", [Nodes]),
            mnesia:delete_table(?PUB_AGG_RECORD_NAME),
            mnesia:create_table(?PUB_AGG_RECORD_NAME, [
                {type, bag}, 
                {ram_copies, Nodes}, 
                {attributes, record_info(fields, ?PUB_AGG_RECORD_NAME)}
            ]),
            wait_for_tables();
        false ->
            ps_bench_utils:log_message("Waiting for mnesia tables to initialize. You may see an exit call for mnesia, this is fine.", []),
            wait_for_tables()
    end.

%% It's possible mnesia will be stopped by the master node while waiting, which will cause an exception
%% This function swallows the exception and retries until we're ready
wait_for_tables() ->
    process_flag(trap_exit, true),
    case mnesia:wait_for_tables([?PUB_AGG_RECORD_NAME], 5000) of
        {timeout, _} ->
            wait_for_tables();
        {error, {node_not_running, _}} ->
            timer:sleep(500),
            wait_for_tables();
        ok ->
            process_flag(trap_exit, false)
    end.

wait_for_mnesia_on_nodes([]) -> ok;
wait_for_mnesia_on_nodes(Nodes) ->
    NotReady = lists:filter(fun(Node) ->
        case rpc:call(Node, mnesia, system_info, [is_running], 5000) of
            yes -> false;
            _ -> true
        end
    end, Nodes),
    case NotReady of
        [] -> ok;
        _ ->
            timer:sleep(500),
            wait_for_mnesia_on_nodes(NotReady)
    end.

%% Create ETS tables if they don't already exist (safe to call many times)
ensure_tables() ->
    %% per-topic pub seq
    case ets:info(?T_PUBSEQ) of
        undefined -> ets:new(?T_PUBSEQ, [named_table, public, set,
                                         {read_concurrency,true},{write_concurrency,true}]);
        _ -> ok
    end,
    %% last recv seq per topic
    case ets:info(?T_RECVSEQ) of
        undefined -> ets:new(?T_RECVSEQ,[named_table, public, set,
                                         {read_concurrency,true},{write_concurrency,true}]);
        _ -> ok
    end,
    %% events (ordered by recv timestamp)
    case ets:info(?T_CONNECT_EVENTS) of
        undefined -> ets:new(?T_CONNECT_EVENTS, [named_table, public, ordered_set,
                                                 {write_concurrency,true}]);
        _ -> ok
    end,
    case ets:info(?T_DISCONNECT_EVENTS) of
        undefined -> ets:new(?T_DISCONNECT_EVENTS, [named_table, public, ordered_set,
                                                    {write_concurrency,true}]);
        _ -> ok
    end,
    case ets:info(?T_PUBLISH_EVENTS) of
        undefined -> ets:new(?T_PUBLISH_EVENTS, [named_table, public, duplicate_bag,
                                                 {write_concurrency,true}]);
        _ -> ok
    end,
    case ets:info(?T_RECV_EVENTS) of
        undefined -> ets:new(?T_RECV_EVENTS, [named_table, public, duplicate_bag,
                                              {write_concurrency,true}]);
        _ -> ok
    end,
    case ets:info(?T_CPU_EVENTS) of
        undefined -> ets:new(?T_CPU_EVENTS, [named_table, public, duplicate_bag,
                                              {write_concurrency,true}]);
        _ -> ok
    end,
    case ets:info(?T_MEM_EVENTS) of
        undefined -> ets:new(?T_MEM_EVENTS, [named_table, public, duplicate_bag,
                                              {write_concurrency,true}]);
        _ -> ok
    end,
    ok.

%% record a recv event 
%% EventMap shape:
%% #{topic=>Topic, seq=>Seq|undefined, t_pub_ns=>TPub|undefined, t_recv_ns=>TRecv, bytes=>Bytes}
record_recv(RecvClientName, TopicBin, Seq, TPubNs, TRecvNs, Bytes, PublisherID) ->
    ensure_tables(),
    {ok, NodeName} = ps_bench_config_manager:fetch_node_name(),
    Event = {NodeName, RecvClientName, PublisherID, TopicBin, Seq, TPubNs, TRecvNs, Bytes},
    ets:insert(?T_RECV_EVENTS, Event),
    ok.

%% Keep old signature for backward compatibility
record_recv(ClientName, TopicBin, Seq, TPubNs, TRecvNs, Bytes) ->
    record_recv(ClientName, TopicBin, Seq, TPubNs, TRecvNs, Bytes, unknown).

record_publish(ClientName, Topic, Seq) ->
    ensure_tables(),
    {ok, NodeName} = ps_bench_config_manager:fetch_node_name(),
    Event = {NodeName, ClientName, Topic, Seq},
    ets:insert(?T_PUBLISH_EVENTS, Event),
    ok.

record_connect(ClientName, TimeNs) ->
    ensure_tables(),
    Event = {TimeNs, ClientName, connect},
    ets:insert(?T_CONNECT_EVENTS, Event),
    
    % Reset sequence tracking for this client on connect
    % AllKeys = ets:select(?T_RECVSEQ, [{{'$1', '_'}, [], ['$1']}]),
    % lists:foreach(fun({C, _, _} = K) when C =:= ClientName -> 
    %                   ets:delete(?T_RECVSEQ, K);
    %                  (_) -> ok 
    %               end, AllKeys),
    % ps_bench_utils:log_message("Client ~p connected at ~p", [ClientName, TimeNs]),
    ok.

record_disconnect(ClientName, TimeNs, Type) ->
    ensure_tables(),
    Event = {TimeNs, ClientName, disconnect, Type},
    ets:insert(?T_DISCONNECT_EVENTS, Event),
    ok.

record_cpu_usage(NodeType, CpuUsage, TimeNs) ->
    ensure_tables(),
    ets:insert(?T_CPU_EVENTS, {TimeNs, NodeType, CpuUsage}).

record_memory_usage(NodeType, MemoryUsage, TimeNs) ->
    ensure_tables(),
    ets:insert(?T_MEM_EVENTS, {TimeNs, NodeType, MemoryUsage}).

fetch_recv_events() ->
    ets:tab2list(?T_RECV_EVENTS).

fetch_recv_events_by_filter(ObjectFilter) ->
    ets:match_object(?T_RECV_EVENTS, ObjectFilter).

write_recv_events_to_disk(OutFile) ->

    % Fetch all events
    RecvEvents = fetch_recv_events(),

    % Open file and write the results
    {ok, File} = file:open(OutFile, [write]),
    io:format(File, "ReceiverID,ReceivingClient,SenderID,Topic,SeqId,PubTimeNs,RecvTimeNs,BytesRecv~n", []),
    lists:foreach(
         fun({NodeName, RecvClientName, PublisherID, TopicBin, Seq, TPubNs, TRecvNs, Bytes}) ->
              io:format(File, "~p,~p,~p,~p,~p,~p,~p,~p~n",[NodeName, RecvClientName, PublisherID, TopicBin, Seq, TPubNs, TRecvNs, Bytes])
          end, RecvEvents),
    % Ensure data is written to disk; ignore errors on platforms where sync is not supported
    _ = file:sync(File),
    file:close(File).

fetch_publish_events() ->
    ets:tab2list(?T_PUBLISH_EVENTS).

fetch_publish_events_by_filter(ObjectFilter) ->
    ets:match_object(?T_PUBLISH_EVENTS, ObjectFilter).

write_publish_events_to_disk(OutFile) ->

    % Fetch all events
    PubEvents = fetch_publish_events(),

    % Open file and write the results
    {ok, File} = file:open(OutFile, [write]),
    io:format(File, "SenderID,SendingClient,Topic,SeqId~n", []),
    lists:foreach(
         fun({NodeName, ClientName, Topic, Seq}) ->
              io:format(File, "~p,~p,~p,~p~n",[NodeName, ClientName, Topic, Seq])
          end, PubEvents),
    % Ensure data is written to disk; ignore errors on platforms where sync is not supported
    _ = file:sync(File),
    file:close(File).

store_aggregate_publish_results_in_mnesia() ->
    % On each node, we want to be able to correlate:
    % - Sender ID
    % - Topic Name
    % - Sequence ID
    % to ensure that each expected message was received

    % We don't need to send all message details, so we can aggregate into one tuple
    % organized as {SenderID, TopicName -> [SeqIds])
    FullTopicMap = ets:foldl(fun({_NodeName, _ClientName, Topic, Seq}, TopicMap) ->
        
        case maps:get(Topic, TopicMap, undefined) of

            % We haven't seen this topic yet
            undefined ->
                TopicMap#{Topic => [Seq]};

            % We already have this topic
            SeqList ->
                NewSeqList = SeqList ++ [Seq],
                TopicMap#{Topic => NewSeqList}
            end
        end, #{}, ?T_PUBLISH_EVENTS),

    % Seralize for mnesia storage
    ResultsBinary = erlang:term_to_binary(FullTopicMap, [compressed]),

    % Store in mnesia
    F = fun() ->
        {ok, NodeName} = ps_bench_config_manager:fetch_node_name(),
        mnesia:write(#?PUB_AGG_RECORD_NAME{node_name=NodeName, binary_aggregate=ResultsBinary})
    end,
    ok = mnesia:activity(transaction, F).

fetch_mnesia_publish_aggregation() ->
    F = fun() -> mnesia:match_object(?PUB_AGG_RECORD_NAME, mnesia:table_info(?PUB_AGG_RECORD_NAME, wild_pattern), read) end,
    mnesia:activity(transaction, F).

fetch_mnesia_publish_aggregation_from_node(NodeName) ->
    F = fun() -> mnesia:match_object(?PUB_AGG_RECORD_NAME, {?PUB_AGG_RECORD_NAME, NodeName, '_'}, read) end,
    mnesia:activity(transaction, F).

fetch_connect_events() ->
    ets:tab2list(?T_CONNECT_EVENTS).

write_connect_events_to_disk(OutFile) ->

    % Fetch all events
    ConnectEvents = fetch_connect_events(),

    % Open file and write the results
    {ok, File} = file:open(OutFile, [write]),
    io:format(File, "ConnectTimeNs,ClientName,Op~n", []),
    lists:foreach(
         fun({TimeNs, ClientName, Op}) ->
              io:format(File, "~p,~p,~p~n",[TimeNs, ClientName, Op])
          end, ConnectEvents),
    % Ensure data is written to disk; ignore errors on platforms where sync is not supported
    _ = file:sync(File),
    file:close(File).

fetch_disconnect_events() ->
    ets:tab2list(?T_DISCONNECT_EVENTS).

write_disconnect_events_to_disk(OutFile) ->

    % Fetch all events
    DisconnectEvents = fetch_disconnect_events(),

    % Open file and write the results
    {ok, File} = file:open(OutFile, [write]),
    io:format(File, "DisconnectTimeNs,ClientName,Op,Type~n", []),
    lists:foreach(
         fun({TimeNs, ClientName, Op, Type}) ->
              io:format(File, "~p,~p,~p,~p~n",[TimeNs, ClientName, Op, Type])
          end, DisconnectEvents),
    % Ensure data is written to disk; ignore errors on platforms where sync is not supported
    _ = file:sync(File),
    file:close(File).

fetch_cpu_usage() ->
    ets:match_object(?T_CPU_EVENTS, {'_', local, '_'}).

fetch_broker_cpu_usage() ->
    ets:match_object(?T_CPU_EVENTS, {'_', broker, '_'}).

fetch_memory_usage() ->
    ets:match_object(?T_MEM_EVENTS, {'_', local, '_'}).

fetch_broker_memory_usage() ->
    ets:match_object(?T_MEM_EVENTS, {'_', broker, '_'}).

write_cpu_usage_events_to_disk(OutFile) ->
    % Fetch all events
    CPUEvents = ets:tab2list(?T_CPU_EVENTS),

    % Open file and write the results
    {ok, File} = file:open(OutFile, [write]),
    io:format(File, "TimeNs,Type,Value~n", []),
    lists:foreach(
         fun({TimeNs, NodeType, CpuUsage}) ->
              io:format(File, "~p,~p,~p~n",[TimeNs, NodeType, CpuUsage])
          end, CPUEvents),
    % Ensure data is written to disk; ignore errors on platforms where sync is not supported
    _ = file:sync(File),
    file:close(File).

write_memory_usage_events_to_disk(OutFile) ->
    % Fetch all events
    MemEvents = ets:tab2list(?T_MEM_EVENTS),

    % Open file and write the results
    {ok, File} = file:open(OutFile, [write]),
    io:format(File, "TimeNs,Type,Value~n", []),
    lists:foreach(
         fun({TimeNs, NodeType, MemUsage}) ->
              io:format(File, "~p,~p,~p~n",[TimeNs, NodeType, MemUsage])
          end, MemEvents),
    % Ensure data is written to disk; ignore errors on platforms where sync is not supported
    _ = file:sync(File),
    file:close(File).

get_last_recv_seq(TopicBin) ->
    case ets:lookup(?T_RECVSEQ, {recv_topic, TopicBin}) of
        [{{recv_topic, TopicBin}, Last}] -> Last;
        [] -> 0
    end.

put_last_recv_seq(TopicBin, Seq) ->
    ensure_tables(),
    ets:insert(?T_RECVSEQ, {{recv_topic, TopicBin}, Seq}),
    ok.
