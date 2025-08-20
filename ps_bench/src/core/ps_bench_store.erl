-module(ps_bench_store).

-include("ps_bench_config.hrl").

-export([initialize_node_storage/0]).
%% seq mgmt
-export([get_next_seq_id/1]).
%% recv event I/O
-export([record_recv/6, record_connect/2, record_disconnect/2]).
%% rollup helpers
-export([take_events_until/1, get_last_recv_seq/1, put_last_recv_seq/2]).
%% window summaries
-export([put_window_summary/3, list_window_summaries/0]).


-define(T_PUBSEQ, psb_pub_seq).      %% {pub_topic, TopicBin} -> Seq
-define(T_RECVSEQ, psb_recv_seq).    %% {recv_topic, TopicBin} -> LastSeqSeen
-define(T_CONNECT, psb_recv_seq).    %% 
-define(T_CONNECT_EVENTS, psb_connect_events).
-define(T_DISCONNECT_EVENTS, psb_disconnect_events).
-define(T_PUBLISH_EVENTS, psb_publish_events).       %% ordered by t_recv_ns key
-define(T_RECV_EVENTS, psb_recv_events).
-define(T_WINS, psb_windows).        %% {{RunId, WinStartMs}} -> SummaryMap

initialize_node_storage() ->
    ets:new(?T_PUBSEQ, [named_table, public, set, {read_concurrency,true},{write_concurrency,true}]),
    ets:new(?T_RECVSEQ,[named_table, public, set, {read_concurrency,true},{write_concurrency,true}]),
    %% keep events ordered by receive timestamp (ns)
    ets:new(?T_CONNECT_EVENTS, [named_table, public, ordered_set, {write_concurrency,true}]),
    ets:new(?T_DISCONNECT_EVENTS, [named_table, public, ordered_set, {write_concurrency,true}]),
    ets:new(?T_PUBLISH_EVENTS, [named_table, public, ordered_set, {write_concurrency,true}]),
    ets:new(?T_RECV_EVENTS, [named_table, public, ordered_set, {write_concurrency,true}]),
    ets:new(?T_WINS,   [named_table, public, set, {write_concurrency,true}]),

    % Setup mnesia DB
    % io:format("STARTING"),
    % application:start(mnesia),
    % mnesia:create_table(?RECV_EVENT_RECORD_NAME, [{attributes, record_info(fields, ?RECV_EVENT_RECORD_NAME)},
    %                                                 {type, bag},
    %                                                 {index, #?RECV_EVENT_RECORD_NAME.topic}]),
    % io:format("AFTER"),
    ok.

%% publisher seq generation (per-topic) 
get_next_seq_id(Topic) ->
    Key = {pub_topic, Topic},
    ets:update_counter(?T_PUBSEQ, Key, {2,1}, {Key,0}).

%% record a recv event 
%% EventMap shape:
%% #{topic=>Topic, seq=>Seq|undefined, t_pub_ns=>TPub|undefined, t_recv_ns=>TRecv, bytes=>Bytes}
record_recv(_ClientName, TopicBin, Seq, TPubNs, TRecvNs, Bytes) ->

    Key = TRecvNs,   %% ordered_set key
    Event = #{topic=>TopicBin, seq=>Seq, t_pub_ns=>TPubNs, t_recv_ns=>TRecvNs, bytes=>Bytes},

    % Record = #?RECV_EVENT_RECORD_NAME{node_name=node(), client_name=ClientName, topic=TopicBin, seq_id=Seq, pub_timestamp=TPubNs, recv_timestamp=TRecvNs, payload_size=Bytes},
    % mnesia:write(Record),

    % io:format("~p~n", [mnesia:table_info(?RECV_EVENT_RECORD_NAME, all)]),

    ets:insert(?T_RECV_EVENTS, {Key, Event}),
    ok.

record_connect(_ClientName, _TimeNs) ->
    ok.

record_disconnect(_ClientName, _TimeNs) ->
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
    ets:insert(?T_RECVSEQ, {{recv_topic, TopicBin}, Seq}),
    ok.

%% window summaries 
put_window_summary(RunId, WinStartMs, Summary) ->
    % ps_bench_utils:log_message("Results: ~p~n", [Summary]),
    ets:insert(?T_WINS, {{RunId, WinStartMs}, Summary}), ok.

list_window_summaries() ->
    lists:sort(ets:tab2list(?T_WINS)).
    
