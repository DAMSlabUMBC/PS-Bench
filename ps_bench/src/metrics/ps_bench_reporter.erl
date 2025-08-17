-module(ps_bench_reporter).
-export([write_csv/0, write_csv/1]).

write_csv() ->
    write_csv("metrics_out.csv").

write_csv(Path) ->
    Windows = ps_bench_store:list_windows(),
    {ok, F} = file:open(Path, [write]),
    io:format(F, "ts_start_ns,ts_end_ns,msgs,bytes,msgs_per_s~n", []),
    lists:foreach(
      fun(#{ts_start := S, ts_end := E, msgs := M, bytes := B, msgs_per_s := R}) ->
          io:format(F, "~B,~B,~B,~B,~B~n", [S,E,M,B,R])
      end, Windows),
    file:close(F), ok.
