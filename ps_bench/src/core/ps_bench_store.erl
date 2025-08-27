-module(ps_bench_store).

-include("ps_bench_config.hrl").

-export([initialize_node_storage/0]).
%% seq mgmt
-export([get_next_seq_id/1]).
%% recv event I/O
-export([record_recv/6, record_connect/2, record_disconnect/3]).
%% rollup helpers
-export([take_events_until/1, get_last_recv_seq/1, put_last_recv_seq/2]).
%% window summaries
-export([put_window_summary/3, list_window_summaries/0, get_dropped_message_count/0]).


-define(T_PUBSEQ, psb_pub_seq).      %% {pub_topic, TopicBin} -> Seq
-define(T_RECVSEQ, psb_recv_seq).    %% {recv_topic, TopicBin} -> LastSeqSeen
-define(T_CONNECT, psb_recv_seq).    %% 
-define(T_CONNECT_EVENTS, psb_connect_events).
-define(T_DISCONNECT_EVENTS, psb_disconnect_events).
-define(T_PUBLISH_EVENTS, psb_publish_events).       %% ordered by t_recv_ns key
-define(T_RECV_EVENTS, psb_recv_events).
-define(T_WINS, psb_windows).        %% {{RunId, WinStartMs}} -> SummaryMap
-define(T_DROPPED, psb_dropped_msgs).

initialize_node_storage() ->
    ensure_tables(),
    ok.

%% publisher seq generation (per-topic) 
get_next_seq_id(Topic) ->
    ensure_tables(),
    Key = {pub_topic, Topic},
    ets:update_counter(?T_PUBSEQ, Key, {2,1}, {Key,0}).

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
        undefined -> ets:new(?T_RECV_EVENTS, [named_table, public, ordered_set,
                                              {write_concurrency,true}]);
        _ -> ok
    end,
    case ets:info(?T_WINS) of
        undefined -> ets:new(?T_WINS, [named_table, public, set,
                                       {write_concurrency,true}]);
        _ -> ok
    end,
    case ets:info(?T_DROPPED) of
        undefined -> ets:new(?T_DROPPED, [named_table, public, set,
                                        {write_concurrency,true}]);
        _ -> ok
    end,
    ok.

%% Track dropped messages based on sequence gaps
check_and_record_drops(ClientName, TopicBin, PublisherID, Seq) ->
    ensure_tables(),
    Key = {ClientName, TopicBin, PublisherID},
    
    case ets:lookup(?T_RECVSEQ, Key) of
        [] -> 
            % First message from this publisher
            ets:insert(?T_RECVSEQ, {Key, Seq}),
            0;  % No drops
        [{Key, LastSeq}] when Seq > LastSeq + 1 ->
            % We have drops
            DroppedCount = Seq - LastSeq - 1,
            ets:update_counter(?T_DROPPED, Key, {2, DroppedCount}, {Key, 0}),
            ets:insert(?T_RECVSEQ, {Key, Seq}),
            ps_bench_utils:log_message("Drops detected: Publisher ~p dropped ~p messages", [PublisherID, DroppedCount]),
            DroppedCount;
        [{Key, _LastSeq}] ->
            % Sequential message, no drops
            ets:insert(?T_RECVSEQ, {Key, Seq}),
            0
    end.

%% record a recv event 
%% EventMap shape:
%% #{topic=>Topic, seq=>Seq|undefined, t_pub_ns=>TPub|undefined, t_recv_ns=>TRecv, bytes=>Bytes}
record_recv(ClientName, TopicBin, Seq, TPubNs, TRecvNs, Bytes, PublisherID) ->
    ensure_tables(),
    
    % Check for drops
    check_and_record_drops(ClientName, TopicBin, PublisherID, Seq),
    
    Key = TRecvNs,   %% ordered_set key
    Event = #{topic=>TopicBin, seq=>Seq, publisher=>PublisherID,
              t_pub_ns=>TPubNs, t_recv_ns=>TRecvNs, bytes=>Bytes},

    ets:insert(?T_RECV_EVENTS, {Key, Event}),
    ok.

%% Keep old signature for backward compatibility
record_recv(ClientName, TopicBin, Seq, TPubNs, TRecvNs, Bytes) ->
    record_recv(ClientName, TopicBin, Seq, TPubNs, TRecvNs, Bytes, unknown).

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
    ps_bench_utils:log_message("Client ~p connected at ~p", [ClientName, TimeNs]),
    ok.

record_disconnect(ClientName, TimeNs, Type) ->
    ensure_tables(),
    Key = {TimeNs, ClientName, disconnect},
    ets:insert(?T_DISCONNECT_EVENTS, {Key, #{client=>ClientName, 
                                              time=>TimeNs, 
                                              type=>Type}}),
    ps_bench_utils:log_message("Client ~p disconnected (~p) at ~p", [ClientName, Type, TimeNs]),
    ok.

%% Drain all events with t_recv_ns <= CutoffNs and delete them
take_events_until(CutoffNs) when is_integer(CutoffNs) ->
    take_events_until_(ets:first(?T_RECV_EVENTS), CutoffNs, []).

take_events_until_('$end_of_table', _Cutoff, Acc) ->
    lists:reverse(Acc);
take_events_until_(Key, Cutoff, Acc) when Key =< Cutoff ->
    [{Key, Event}] = ets:lookup(?T_RECV_EVENTS, Key),
    ets:delete(?T_RECV_EVENTS, Key),
    take_events_until_(ets:next(?T_RECV_EVENTS, Key), Cutoff, [Event|Acc]);
take_events_until_(Key, _Cutoff, Acc) when Key > _Cutoff ->
    lists:reverse(Acc).

get_last_recv_seq(TopicBin) ->
    case ets:lookup(?T_RECVSEQ, {recv_topic, TopicBin}) of
        [{{recv_topic, TopicBin}, Last}] -> Last;
        [] -> 0
    end.

put_last_recv_seq(TopicBin, Seq) ->
    ensure_tables(),
    ets:insert(?T_RECVSEQ, {{recv_topic, TopicBin}, Seq}),
    ok.

%% window summaries 
put_window_summary(RunId, WinStartMs, Summary) ->
    ensure_tables(),
    % ps_bench_utils:log_message("Results: ~p~n", [Summary]),
    ets:insert(?T_WINS, {{RunId, WinStartMs}, Summary}), ok.

list_window_summaries() ->
    lists:sort(ets:tab2list(?T_WINS)).

%% Get total dropped message count
get_dropped_message_count() ->
    ensure_tables(),
    case ets:info(?T_DROPPED) of
        undefined -> 0;
        _ -> lists:sum([Count || {_Key, Count} <- ets:tab2list(?T_DROPPED)])
    end.
    
