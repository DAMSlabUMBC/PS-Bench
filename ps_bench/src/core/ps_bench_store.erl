-module(ps_bench_store).
-behaviour(gen_server).

-export([start_link/0, init/1]).
%% seq mgmt
-export([next_seq/1]).
%% recv event I/O
-export([record_recv/5]).
%% rollup helpers
-export([take_events_until/1, get_last_recv_seq/1, put_last_recv_seq/2]).
%% window summaries
-export([put_window_summary/3]).

-define(T_PUBSEQ, psb_pub_seq).      %% {pub_topic, TopicBin} -> Seq
-define(T_RECVSEQ, psb_recv_seq).    %% {recv_topic, TopicBin} -> LastSeqSeen
-define(T_EVENTS, psb_events).       %% ordered by t_recv_ns key
-define(T_WINS, psb_windows).        %% {{RunId, WinStartMs}} -> SummaryMap

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    ets:new(?T_PUBSEQ, [named_table, public, set, {read_concurrency,true},{write_concurrency,true}]),
    ets:new(?T_RECVSEQ,[named_table, public, set, {read_concurrency,true},{write_concurrency,true}]),
    %% keep events ordered by receive timestamp (ns)
    ets:new(?T_EVENTS, [named_table, public, ordered_set, {write_concurrency,true}]),
    ets:new(?T_WINS,   [named_table, public, set, {write_concurrency,true}]),
    {ok, #{}}.

%% publisher seq generation (per-topic) 
next_seq(TopicBin) when is_binary(TopicBin) ->
    Key = {pub_topic, TopicBin},
    ets:update_counter(?T_PUBSEQ, Key, {2,1}, {Key,0}).

%% record a recv event 
%% EventMap shape:
%% #{topic=>Topic, seq=>Seq|undefined, t_pub_ns=>TPub|undefined, t_recv_ns=>TRecv, bytes=>Bytes}
record_recv(TopicBin, Seq, TPubNs, TRecvNs, Bytes) ->
    Key = TRecvNs,   %% ordered_set key
    Event = #{topic=>TopicBin, seq=>Seq, t_pub_ns=>TPubNs, t_recv_ns=>TRecvNs, bytes=>Bytes},
    ets:insert(?T_EVENTS, {Key, Event}),
    ok.

%% Drain all events with t_recv_ns <= CutoffNs and delete them
take_events_until(CutoffNs) when is_integer(CutoffNs) ->
    take_events_until_(ets:first(?T_EVENTS), CutoffNs, []).

take_events_until_('$end_of_table', _Cutoff, Acc) ->
    lists:reverse(Acc);
take_events_until_(Key, Cutoff, Acc) when Key =< Cutoff ->
    [{Key, Event}] = ets:lookup(?T_EVENTS, Key),
    ets:delete(?T_EVENTS, Key),
    take_events_until_(ets:next(?T_EVENTS, Key), Cutoff, [Event|Acc]);
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
    ets:insert(?T_WINS, {{RunId, WinStartMs}, Summary}), ok.

%% gen_server boilerplate
handle_call(_,_,S)->{reply,ok,S}.
handle_cast(_,S)->{noreply,S}.
handle_info(_,S)->{noreply,S}.
terminate(_,_) -> ok.
code_change(_,S,_) -> {ok,S}.
