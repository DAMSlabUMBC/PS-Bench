-module(ps_bench_store).

-include("ps_bench_config.hrl").

-export([initialize_node_storage/0, initialize_mnesia_storage/1]).
%% seq mgmt
-export([get_next_seq_id/1]).
%% recv event I/O
-export([record_recv/6, record_recv/7, record_publish/3, record_connect/2, record_disconnect/3]).
%% rollup helpers
-export([get_last_recv_seq/1, put_last_recv_seq/2]).
%% window summaries
-export([fetch_recv_events/0, fetch_recv_events_by_filter/1, fetch_publish_events/0, fetch_publish_events_from_node/1, fetch_connect_events/0, fetch_disconnect_events/0]).


-define(T_PUBSEQ, psb_pub_seq).      %% {pub_topic, TopicBin} -> Seq
-define(T_RECVSEQ, psb_recv_seq).    %% {recv_topic, TopicBin} -> LastSeqSeen
-define(T_CONNECT, psb_recv_seq).    %% 
-define(T_CONNECT_EVENTS, psb_connect_events).
-define(T_DISCONNECT_EVENTS, psb_disconnect_events).
-define(T_PUBLISH_EVENTS, psb_publish_events).       %% ordered by t_recv_ns key
-define(T_RECV_EVENTS, psb_recv_events).

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
            % We don't store anything on disk, so we don't need to create a schema
            % Make sure mnesia is started and create the table
            ps_bench_utils:log_message("Initializing mnesia schema on ~p. You may see an exit call for mnesia, this is fine.", [Nodes]),
            rpc:multicall(Nodes, application, stop, [mnesia]),
            mnesia:create_schema(Nodes),
            ps_bench_utils:log_message("Attempting to restart mnesia database on ~p.", [Nodes]),
            rpc:multicall(Nodes, application, start, [mnesia]),
            lists:foreach(fun(Node) -> mnesia:add_table_copy(schema, Node, ram_copies) end, Nodes -- [node()]),
            rpc:multicall(Nodes -- [node()], mnesia, change_config, [extra_db_nodes, Nodes]),
            ps_bench_utils:log_message("Creating mnesia tables on ~p", [Nodes]),
            mnesia:delete_table(?PUB_EVENT_RECORD_NAME),
            mnesia:create_table(?PUB_EVENT_RECORD_NAME, [{type, bag}, {ram_copies, Nodes}, {attributes, record_info(fields, ?PUB_EVENT_RECORD_NAME)}]);
        false ->
            ps_bench_utils:log_message("Waiting for mnesia tables to initialize. You may see an exit call for mnesia, this is fine.", []),
            wait_for_tables()
    end.

%% It's possible mnesia will be stopped by the master node while waiting, which will cause an exception
%% This function swallows the exception and retries until we're ready
wait_for_tables() ->
    process_flag(trap_exit, true),
    case mnesia:wait_for_tables([?PUB_EVENT_RECORD_NAME], infinity) of
        {timeout, _} ->
            wait_for_tables();
        {error, {node_not_running, _}} ->
            wait_for_tables();
        ok ->
            process_flag(trap_exit, false)
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
        undefined -> ets:new(?T_PUBLISH_EVENTS, [named_table, public, ordered_set,
                                                 {write_concurrency,true}]);
        _ -> ok
    end,
    case ets:info(?T_RECV_EVENTS) of
        undefined -> ets:new(?T_RECV_EVENTS, [named_table, public, duplicate_bag,
                                              {write_concurrency,true}]);
        _ -> ok
    end,
    ok.

%% record a recv event 
%% EventMap shape:
%% #{topic=>Topic, seq=>Seq|undefined, t_pub_ns=>TPub|undefined, t_recv_ns=>TRecv, bytes=>Bytes}
record_recv(RecvClientName, TopicBin, Seq, TPubNs, TRecvNs, Bytes, PublisherID) ->
    ensure_tables(),
    

    Key = TRecvNs,   %% ordered_set key
    {ok, NodeName} = ps_bench_config_manager:fetch_node_name(),
    % Event = #{recv_node=>NodeName, subscriber=>ClientName, topic=>TopicBin, seq=>Seq, publisher=>PublisherID,
    %           t_pub_ns=>TPubNs, t_recv_ns=>TRecvNs, bytes=>Bytes},
    Event = {NodeName, RecvClientName, PublisherID, TopicBin, Seq, TPubNs, TRecvNs, Bytes},

    ets:insert(?T_RECV_EVENTS, Event),
    ok.

%% Keep old signature for backward compatibility
record_recv(ClientName, TopicBin, Seq, TPubNs, TRecvNs, Bytes) ->
    record_recv(ClientName, TopicBin, Seq, TPubNs, TRecvNs, Bytes, unknown).

record_publish(ClientName, Topic, Seq) ->
    F = fun() ->
        {ok, NodeName} = ps_bench_config_manager:fetch_node_name(),
        mnesia:write(#?PUB_EVENT_RECORD_NAME{node_name=NodeName, client_name=ClientName, topic=Topic, seq_id=Seq})
    end,
    ok = mnesia:activity(transaction, F).

record_connect(ClientName, TimeNs) ->
    ensure_tables(),
    Key = {TimeNs, ClientName, connect},
    ets:insert(?T_CONNECT_EVENTS, {Key, #{client=>ClientName, time=>TimeNs}}),
    
    % Reset sequence tracking for this client on connect
    AllKeys = ets:select(?T_RECVSEQ, [{{'$1', '_'}, [], ['$1']}]),
    lists:foreach(fun({C, _, _} = K) when C =:= ClientName -> 
                      ets:delete(?T_RECVSEQ, K);
                     (_) -> ok 
                  end, AllKeys),
    % ps_bench_utils:log_message("Client ~p connected at ~p", [ClientName, TimeNs]),
    ok.

record_disconnect(ClientName, TimeNs, Type) ->
    ensure_tables(),
    Key = {TimeNs, ClientName, disconnect},
    ets:insert(?T_DISCONNECT_EVENTS, {Key, #{client=>ClientName, 
                                              time=>TimeNs, 
                                              type=>Type}}),
    % ps_bench_utils:log_message("Client ~p disconnected (~p) at ~p", [ClientName, Type, TimeNs]),
    ok.

fetch_recv_events() ->
    ets:tab2list(?T_RECV_EVENTS).

fetch_recv_events_by_filter(ObjectFilter) ->
    ets:match_object(?T_RECV_EVENTS, ObjectFilter).

fetch_publish_events() ->
    % Publish events are in a mnesia table
    F = fun() -> mnesia:match_object(?PUB_EVENT_RECORD_NAME, mnesia:table_info(?PUB_EVENT_RECORD_NAME, wild_pattern), read) end,
    mnesia:activity(transaction, F).

fetch_publish_events_from_node(NodeName) ->
    % Publish events are in a mnesia table
    F = fun() -> mnesia:match_object(?PUB_EVENT_RECORD_NAME, {?PUB_EVENT_RECORD_NAME, NodeName, '_', '_', '_'}, read) end,
    mnesia:activity(transaction, F).

fetch_connect_events() ->
    ets:tab2list(?T_CONNECT_EVENTS).

fetch_disconnect_events() ->
    ets:tab2list(?T_DISCONNECT_EVENTS).

get_last_recv_seq(TopicBin) ->
    case ets:lookup(?T_RECVSEQ, {recv_topic, TopicBin}) of
        [{{recv_topic, TopicBin}, Last}] -> Last;
        [] -> 0
    end.

put_last_recv_seq(TopicBin, Seq) ->
    ensure_tables(),
    ets:insert(?T_RECVSEQ, {{recv_topic, TopicBin}, Seq}),
    ok.
