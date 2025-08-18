-module(ps_bench_metrics_rollup).
-behaviour(gen_server).

-include("ps_bench_config.hrl").
-export([start_link/0, write_csv/0, write_csv/1]).
-export([init/1, handle_info/2, handle_call/3, handle_cast/2, terminate/2, code_change/3]).

-record(state, {win_ms=1000, run_id, next_tick_ref}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    {ok, TestName} = ps_bench_config_manager:fetch_test_name(),
    WinMs = case ps_bench_config_manager:fetch_optional(?WINDOW_MS_PROP) of
                {ok, W} when is_integer(W) -> W;
                _ -> ?DEFAULT_WINDOW_MS
            end,
    Ref = erlang:send_after(WinMs, self(), tick),
    {ok, #state{win_ms=WinMs, run_id=TestName, next_tick_ref=Ref}}.

handle_info(tick, S=#state{win_ms=WinMs, run_id=RunId}) ->
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
