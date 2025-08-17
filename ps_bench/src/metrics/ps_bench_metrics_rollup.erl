-module(ps_bench_metrics_rollup).
-behaviour(gen_server).

-include("ps_bench_config.hrl").
-export([start_link/0, write_csv/0, write_csv/1]).
-export([init/1, handle_info/2, handle_call/3, handle_cast/2, terminate/2, code_change/3]).

-record(state, {win_ms=1000, win_ns, run_id, next_tick_ref}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    {ok, TestName} = ps_bench_config_manager:fetch_test_name(),
    WinMs = case ps_bench_config_manager:fetch_optional(?WINDOW_MS_PROP) of
                {ok, W} when is_integer(W) -> W; _ -> ?DEFAULT_WINDOW_MS
            end,
    WinNs = WinMs * 1000000,
    Ref = erlang:send_after(WinMs, self(), tick),
    {ok, #state{win_ms=WinMs, win_ns=WinNs, run_id=TestName, next_tick_ref=Ref}}.

handle_info(tick, S=#state{win_ms=WinMs, win_ns=WinNs, run_id=RunId}) ->
    Tnow = erlang:monotonic_time(nanosecond),
    %% Drain all events received up to now
    Events = ps_bench_store:take_events_until(Tnow),

    %% Simple local metric: throughput per window
    Msgs   = length(Events),
    Bytes  = lists:sum([maps:get(bytes, E) || E <- Events]),
    Rate   = (Msgs * 1000) div WinMs, %% msgs/sec

    WinStartMs = (Tnow div 1000000) - WinMs,
    Summary = #{msgs => Msgs, bytes => Bytes, msgs_per_s => Rate},
    ok = ps_bench_store:put_window_summary(RunId, WinStartMs, Summary),

    Ref = erlang:send_after(WinMs, self(), tick),
    {noreply, S#state{next_tick_ref=Ref}};

%% file output
write_csv() -> write_csv("metrics_out.csv").

write_csv(Path) ->
    {ok, F} = file:open(Path, [write]),
    io:format(F, "run_id,win_start_ms,msgs,bytes,msgs_per_s~n", []),
    lists:foreach(
      fun({{RunId, WinStartMs}, M}) ->
          io:format(F, "~p,~B,~B,~B,~B~n",
                    [RunId, WinStartMs,
                     maps:get(msgs, M, 0),
                     maps:get(bytes, M, 0),
                     maps:get(msgs_per_s, M, 0)])
      end,
      ps_bench_store:list_window_summaries()),
    file:close(F), ok.


handle_info(_, S) -> {noreply, S}.
handle_call(_,_,S)->{reply,ok,S}.
handle_cast(_,S)->{noreply,S}.
terminate(_,_) -> ok.
code_change(_,S,_) -> {ok,S}.

%% Helpers
build_arrays_and_drops(Events) ->
    %% group by topic for drop accounting
    TopicGroups = lists:foldl(
        fun(E, Acc) ->
            T = maps:get(topic, E),
            maps:update_with(T, fun(L) -> [E|L] end, [E], Acc)
        end, #{}, Events),
    
    %% compute drops by topic, and lat/size arrays across all
    {DropsTotal, LatAll, SizeAll, RecvCount} =
        maps:fold(fun(Topic, Evts, {Dt, Lat, Sz, C}) ->
            %% arrival order ~ t_recv_ns already (we drained ordered_set)
            Evts1 = lists:reverse(Evts),
            {DropsTopic, LastSeq} = drops_for_topic(Topic, Evts1),
            Lat1 = latencies_ms(Evts1),
            Sz1  = [maps:get(bytes, E) || E <- Evts1],
            C1   = C + length(Evts1),
            %% persist last seq for next window
            case LastSeq of
                0 -> ok;
                _ -> ps_bench_store:put_last_recv_seq(Topic, LastSeq)
            end,
            {Dt + DropsTopic, Lat ++ Lat1, Sz ++ Sz1, C1}
        end, {0, [], [], 0}, TopicGroups),
    {LatAll, SizeAll, RecvCount, DropsTotal}.

latencies_ms(Evts) ->
    lists:foldl(fun(E, Acc) ->
        case {maps:get(t_pub_ns, E, undefined), maps:get(t_recv_ns, E)} of
            {undefined, _} -> Acc;
            {TP, TR} when is_integer(TP), is_integer(TR) ->
                [ (TR - TP) div 1000000 | Acc ]
        end
    end, [], Evts).

drops_for_topic(Topic, EvtsAsc) ->
    Prev = ps_bench_store:get_last_recv_seq(Topic),
    drops_for_topic_(Prev, EvtsAsc, 0, Prev).

drops_for_topic_(PrevSeq, [], DropsAcc, LastOut) ->
    {DropsAcc, LastOut};
drops_for_topic_(PrevSeq, [E|Rest], DropsAcc, _LastOut) ->
    Seq = maps:get(seq, E, undefined),
    case Seq of
        undefined -> drops_for_topic_(PrevSeq, Rest, DropsAcc, PrevSeq);
        _ ->
            Gap = Seq - (PrevSeq + 1),
            NewDrops = DropsAcc + (if Gap > 0 -> Gap; true -> 0 end),
            drops_for_topic_(Seq, Rest, NewDrops, Seq)
    end.
