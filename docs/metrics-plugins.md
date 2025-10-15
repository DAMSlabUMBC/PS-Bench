# PSMark Metric Plugins (Erlang)

This guide shows how to create and register Erlang-based metric plugins. Python plugins are scaffolded in the codebase but disabled by default; use Erlang for now.

## Plugin Lifecycle
- PSMark creates a per-run, timestamped output directory under the base `output_dir` (default `results/`). Example: `results/run_20240908_153010_mqtt_single_node_light_runner1/`.
- The metric manager calls `Module:init(OutDir)` once per run, where `OutDir` is that per-run directory. Use it to initialize state or remember `OutDir` (often via `persistent_term`).
- After the scenario completes, the metric manager calls `Module:calc()` for each registered plugin. Do your analysis and write outputs (e.g., CSV) in `calc/0`.

## Data Available for Calculations
From `ps_bench_store`:
- Events
  - `fetch_recv_events/0` → `[{Node, RecvClient, PublisherNode, TopicBin, Seq, PubTimeNs, RecvTimeNs, Bytes}]`
  - `fetch_publish_events/0` → `[{Node, ClientName, TopicBin, Seq}]`
  - `fetch_connect_events/0` → ordered connect events
  - `fetch_disconnect_events/0` → ordered disconnect events
- Filters
  - `fetch_recv_events_by_filter/1`, `fetch_publish_events_by_filter/1` (ETS match_object filters)
- HW stats (if enabled)
  - `fetch_cpu_usage/0`, `fetch_memory_usage/0` (tuples `{local, Pct}`)
  - `fetch_broker_cpu_usage/0`, `fetch_broker_memory_usage/0` (tuples `{broker, Pct}`)

## Minimal Plugin Skeleton
```erlang
-module(my_metric_plugin).
-export([init/1, calc/0]).

init(OutDir) ->
    persistent_term:put({?MODULE, out_dir}, OutDir),
    ok.

calc() ->
    OutDir = persistent_term:get({?MODULE, out_dir}),
    %% Example: compute count of recv events per-publisher
    Events = ps_bench_store:fetch_recv_events(),
    Counts = lists:foldl(
               fun({_, _, PubNode, _Topic, _Seq, _TPub, _TRecv, _Bytes}, Acc) ->
                       maps:update_with(PubNode, fun(N)->N+1 end, 1, Acc)
               end, #{}, Events),
    write_csv(filename:join(OutDir, "my_metric.csv"), Counts).

write_csv(Path, Map) ->
    {ok, F} = file:open(Path, [write]),
    io:format(F, "Publisher,Count~n", []),
    maps:fold(fun(Pub, C, ok) -> io:format(F, "~p,~p~n", [Pub, C]), ok end, ok, Map),
    file:close(F),
    ok.
```

## Registering a Plugin in a Scenario
Add your module to `metric_config.metric_plugins` with the `erlang` interface:
```erlang
{metric_config,
 [
   {output_dir, "results"},
   {metric_plugins,
    [
      {ps_bench_throughput_calc_plugin, erlang},
      {ps_bench_latency_calc_plugin, erlang},
      {my_metric_plugin, erlang}
    ]}
 ]}
```

## Examples
- `ps_bench/src/metrics/plugins/ps_bench_latency_calc_plugin.erl`
- `ps_bench/src/metrics/plugins/ps_bench_throughput_calc_plugin.erl`
- `ps_bench/src/metrics/plugins/ps_bench_dropped_message_calc_plugin.erl`
