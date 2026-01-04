-module(ps_bench_metrics_hw_stats_reader).
-behaviour(gen_server).
-export([start_link/0]).

-include("ps_bench_config.hrl").

-export([init/1, handle_info/2, handle_cast/2, handle_call/3]).
-export([poll_hw_stats/0]).
-define(CPU_LINE_START, "node_cpu_seconds_total").
-define(CPU_LINE_IDLE, "mode=\"idle\"").
-define(MEM_USE_LINE_START, "node_memory_Active_bytes").
-define(MEM_TOTAL_LINE_START, "node_memory_MemTotal_bytes").
-define(HELP_LINE_START, "#").

start_link() -> 
      gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) -> 
      initialize_hw_stats(),
      {ok, #{timer_ref => undefined}}.

handle_call(start_polling, _From, State) -> 
      {ok, PollFrequencyMs} = ps_bench_config_manager:fetch_metric_hw_poll_period(),
      {ok, TRef} =
                timer:apply_repeatedly(
                    PollFrequencyMs,
                    ?MODULE, poll_hw_stats,
                    []),
      {reply, ok, State#{timer_ref := TRef}};

handle_call(stop_polling, _From, State = #{timer_ref := TRef}) -> 
      case TRef of
            undefined ->
                  {reply, ok, State};
            _ ->
                  timer:cancel(TRef),
                  {reply, ok, State}
      end;

handle_call({write_stats, OutDir}, _From, State) -> 

      % Write local first
      FullPath = filename:join(OutDir, "local_hw_stats.csv"),
      calculate_and_write_local_stats(FullPath),

      % Primary node also reads broker stats
      case ps_bench_node_manager:is_primary_node() of
            true ->
                  % Currently only supported for MQTT
                  {ok, ProtocolType} = ps_bench_config_manager:fetch_protocol_type(),
                  case ProtocolType of 
                        ?MQTT_V5_PROTOCOL ->
                              FullBrokerPath = filename:join(OutDir, "broker_hw_stats.csv"),
                              calculate_and_write_broker_stats(FullBrokerPath),
                              {reply, ok, State};
                        ?MQTT_V311_PROTOCOL ->
                              FullBrokerPath = filename:join(OutDir, "broker_hw_stats.csv"),
                              calculate_and_write_broker_stats(FullBrokerPath),
                              {reply, ok, State};
                        _ ->
                              {reply, ok, State}
                  end;
            false ->
                  {reply, ok, State}
      end;

handle_call(_, _From, State) -> {reply, ok, State}.
handle_cast(_, State) -> {noreply, State}.
handle_info(_, State) -> {noreply, State}.

initialize_hw_stats() ->
      case inets:start() of
            ok ->
                  ok;
            {error, {already_started,inets}} ->
                  ok;
            {error, Value} ->
                  io:format("ERROR: Failed to start HW stats reader - ~p", [Value])
      end.

poll_hw_stats() ->
      Url = "http://localhost:9100/metrics",
      fetch_and_store_hw_usage(Url, local),

      % Primary node also reads broker stats
      case ps_bench_node_manager:is_primary_node() of
            true ->
                  % Currently only supported for MQTT
                  {ok, ProtocolType} = ps_bench_config_manager:fetch_protocol_type(),
                  case ProtocolType of 
                        ?MQTT_V5_PROTOCOL ->
                              {ok, BrokerIP, _} = ps_bench_config_manager:fetch_mqtt_broker_information(),
                              BrokerUrl = "http://" ++ BrokerIP ++ ":9100/metrics",
                              fetch_and_store_hw_usage(BrokerUrl, broker),
                              ok;
                        ?MQTT_V311_PROTOCOL ->
                              {ok, BrokerIP, _} = ps_bench_config_manager:fetch_mqtt_broker_information(),
                              BrokerUrl = "http://" ++ BrokerIP ++ ":9100/metrics",
                              fetch_and_store_hw_usage(BrokerUrl, broker),
                              ok;
                        _ ->
                              ok
                  end;
            false ->
                  ok
      end.

fetch_and_store_hw_usage(Url, NodeType) ->
      case query_node_exporter(Url) of
            {error, Reason} ->
                  io:format("ERROR: Could not fetch HW stats with reason ~p", [Reason]),
                  ok;
            {ok, ResponseLines} ->
                  TimeNs = erlang:system_time(nanosecond),
                  parse_and_store_cpu_usage(ResponseLines, NodeType, TimeNs),
                  parse_and_store_memory_usage(ResponseLines, NodeType, TimeNs)
      end.

query_node_exporter(Url) ->
      case httpc:request(get, {Url, []}, [], []) of 
            {error, Reason} ->
                  {error, Reason};
            {ok, {{Version, ResponseCode, ReasonPhrase}, _, _}} when ResponseCode =/= 200 ->
                  {error, {Version, ResponseCode, ReasonPhrase}};
            {ok, {{_, 200, _}, _, Body}} ->
                  Lines = string:tokens(Body, "\n"),
                  {ok, Lines}
      end.

parse_and_store_cpu_usage(NodeExporterResponseLines, NodeType, TimeNs) ->
      CpuLines = lists:filter(fun(Line) -> string:find(Line, ?CPU_LINE_START) =/= nomatch andalso string:find(Line, ?HELP_LINE_START) =:= nomatch end, NodeExporterResponseLines),
      IdleCpuLines = lists:filter(fun(Line) -> string:find(Line, ?CPU_LINE_IDLE) =/= nomatch end, CpuLines),
      ActiveCpuLines = lists:filter(fun(Line) -> string:find(Line, ?CPU_LINE_IDLE) =:= nomatch end, CpuLines),

      ActiveCpuTime = lists:foldl(fun(Line, Total) -> Total + parse_value_from_line(Line) end, 0, ActiveCpuLines),
      IdleCpuTime = lists:foldl(fun(Line, Total) -> Total + parse_value_from_line(Line) end, 0, IdleCpuLines),

      % Get previous loop's values for calculation
      PrevActiveTime = persistent_term:get({?MODULE, cpu_active_time, NodeType}, undefined),
      PrevIdleTime = persistent_term:get({?MODULE, cpu_idle_time, NodeType}, undefined),

      % Store time for next loop
      persistent_term:put({?MODULE, cpu_active_time, NodeType}, ActiveCpuTime),
      persistent_term:put({?MODULE, cpu_idle_time, NodeType}, IdleCpuTime),

      % The first loop doesn't have any values stored, so we skip calculation
      case PrevActiveTime of
            undefined ->
                  ok;
            _ ->
                  DeltaActive = ActiveCpuTime - PrevActiveTime,
                  DeltaIdle = IdleCpuTime - PrevIdleTime,
                  CpuUsage = (DeltaActive / (DeltaActive + DeltaIdle)) * 100,

                  % Store usage metric
                  ps_bench_store:record_cpu_usage(NodeType, CpuUsage, TimeNs)
      end.

parse_and_store_memory_usage(NodeExporterResponseLines, NodeType, TimeNs) ->
      MemUseLine = lists:filter(fun(Line) -> string:find(Line, ?MEM_USE_LINE_START) =/= nomatch andalso string:find(Line, ?HELP_LINE_START) =:= nomatch end, NodeExporterResponseLines),
      MemTotalLine = lists:filter(fun(Line) -> string:find(Line, ?MEM_TOTAL_LINE_START) =/= nomatch andalso string:find(Line, ?HELP_LINE_START) =:= nomatch end, NodeExporterResponseLines),

      MemUseValue = lists:foldl(fun(Line, Total) -> Total + parse_value_from_line(Line) end, 0, MemUseLine),
      MemTotalValue = lists:foldl(fun(Line, Total) -> Total + parse_value_from_line(Line) end, 0, MemTotalLine),
      MemUsagePct = (MemUseValue / MemTotalValue) * 100,
      
      % Store usage metric
      ps_bench_store:record_memory_usage(NodeType, MemUsagePct, TimeNs).

parse_value_from_line(Line) ->
      [_, StrValue] = string:tokens(Line, " "),
      % If the value is an int, the string conversion won't work, try that first
      case string:to_float(StrValue) of
            {error, no_float} ->
                  % Check if the value is an integer
                  case string:to_integer(StrValue) of
                        {error, _} ->
                              io:format("WARNING: Unknown value ~p found in CPU usage calculation", [StrValue]);
                        {IntValue, _} ->
                              IntValue
                  end;
            {FloatValue, _} ->
                  FloatValue
      end.

calculate_hw_stats(StatType, NodeType) ->
      AllStatEvents = case NodeType of
                              local ->
                                    case StatType of 
                                          cpu ->
                                                ps_bench_store:fetch_cpu_usage();
                                          memory ->
                                                ps_bench_store:fetch_memory_usage()
                                    end;
                              broker ->
                                    case StatType of 
                                          cpu ->
                                                ps_bench_store:fetch_broker_cpu_usage();
                                          memory ->
                                                ps_bench_store:fetch_broker_memory_usage()
                                    end
                        end,

      TotalEvents = length(AllStatEvents),
      TotalUsage = lists:foldl(fun({_, _, UsageVal}, Total) -> Total + UsageVal end, 0, AllStatEvents),
      AvgUsage = TotalUsage / TotalEvents,

      % Bootstrap these with out of range values to ensure the first one takes the real value
      MaxUsage = lists:foldl(fun({_, _, UsageVal}, CurrMax) -> max(UsageVal, CurrMax) end, -1, AllStatEvents), 
      MinUsage = lists:foldl(fun({_, _, UsageVal}, CurrMin) -> min(UsageVal, CurrMin) end, 101, AllStatEvents),

      % Now compute sum of squared means
      SumOfSquaredMeans = lists:foldl(fun(Event, CurrSum) -> 
                                                {_, _, UsageVal} = Event,
                                                Difference = UsageVal - AvgUsage,
                                                SquaredDifference = math:pow(Difference, 2),
                                                CurrSum + SquaredDifference
                                          end, 0, AllStatEvents),
      Variance = SumOfSquaredMeans / TotalEvents,

      % Now compute the median value
      SortedEvents = lists:sort(fun(Event1, Event2) ->
                                          {_, _, UsageVal1} = Event1,
                                          {_, _, UsageVal2} = Event2,
                                          UsageVal1 < UsageVal2
                                    end, AllStatEvents),
      MedianIndex = TotalEvents div 2, % Integer divison
      {_, _, MedianUsage} = lists:nth(MedianIndex, SortedEvents),

      % Now compute P90/95/99 values with the sorted events
      % First calculate indexes as (TotalEvents * p_val)
      % e.g. for 500 events, the 90th percentile index would be 450
      % We round down for a more conservative measurement
      P90Index = erlang:trunc(TotalEvents * 0.90),
      P95Index = erlang:trunc(TotalEvents * 0.95),
      P99Index = erlang:trunc(TotalEvents * 0.99),

      {_, _, P90Usage} = lists:nth(P90Index, SortedEvents),
      {_, _, P95Usage} = lists:nth(P95Index, SortedEvents),
      {_, _, P99Usage} = lists:nth(P99Index, SortedEvents),
      
      {MinUsage, MaxUsage, AvgUsage, Variance, MedianUsage, P90Usage, P95Usage, P99Usage}.

calculate_and_write_local_stats(OutFile) ->
      {ok, NodeName} = ps_bench_config_manager:fetch_node_name(),
      CPUResults = calculate_hw_stats(cpu, local),
      MemResults = calculate_hw_stats(memory, local),
      write_stats_to_file(OutFile, NodeName, CPUResults, MemResults).

calculate_and_write_broker_stats(OutFile) ->
      NodeName = "broker",
      CPUResults = calculate_hw_stats(cpu, broker),
      MemResults = calculate_hw_stats(memory, broker),
      write_stats_to_file(OutFile, NodeName, CPUResults, MemResults).

write_stats_to_file(OutFile, NodeName, CPUResults, MemResults) ->
      
      {MinCpuUsage, MaxCpuUsage, AvgCpuUsage, CpuVariance, MedianCpuUsage, P90CpuUsage, P95CpuUsage, P99CpuUsage} = CPUResults,
      {MinMemUsage, MaxMemUsage, AvgMemUsage, MemVariance, MedianMemUsage, P90MemUsage, P95MemUsage, P99MemUsage} = MemResults,

      % Open file and write the results
      {ok, File} = file:open(OutFile, [write]),
      io:format(File, "Node,MinCPUUsage,MaxCPUUsage,AverageCPUUsage,CPUUsageVariance,MedianCPUUsage,P90CPU,P95CPU,P99CPU,MinMemoryUsage,MaxMemoryUsage,AverageMemoryUsage,MemoryUsageVariance,MedianMemoryUsage,P90CMemory,P95Memory,P99Memory~n", []),
      io:format(File, "~p,~p,~p,~p,~p,~p,~p,~p,~p,~p,~p,~p,~p,~p,~p,~p,~p~n",[NodeName, MinCpuUsage, MaxCpuUsage, AvgCpuUsage, CpuVariance, MedianCpuUsage, P90CpuUsage, P95CpuUsage, P99CpuUsage, MinMemUsage, MaxMemUsage, AvgMemUsage, MemVariance, MedianMemUsage, P90MemUsage, P95MemUsage, P99MemUsage]),
	  
	  % Ensure data is written to disk; ignore errors on platforms where sync is not supported
      _ = file:sync(File),
      file:close(File).