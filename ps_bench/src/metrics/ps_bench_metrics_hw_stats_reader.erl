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

handle_call(write_stats, _From, State) -> 
      AvgCpuUsage = calculate_average_cpu_usage(local),
      AvgMemUsage = calculate_average_memory_usage(local),
      ps_bench_utils:log_message("Local CPU Usage - ~p | Local Memory Usage - ~p", [AvgCpuUsage, AvgMemUsage]),

      % Primary node also reads broker stats
      case ps_bench_node_manager:is_primary_node() of
            true ->
                  % Currently only supported for MQTT
                  {ok, ProtocolType} = ps_bench_config_manager:fetch_protocol_type(),
                  case ProtocolType of 
                        ?MQTT_V5_PROTOCOL ->
                              AvgBrokerCpuUsage = calculate_average_cpu_usage(broker),
                              AvgBrokerMemUsage = calculate_average_memory_usage(broker),
                              ps_bench_utils:log_message("Broker CPU Usage - ~p | Broker Memory Usage - ~p", [AvgBrokerCpuUsage, AvgBrokerMemUsage]),
                              {reply, ok, State};
                        ?MQTT_V311_PROTOCOL ->
                              AvgBrokerCpuUsage = calculate_average_cpu_usage(broker),
                              AvgBrokerMemUsage = calculate_average_memory_usage(broker),
                              ps_bench_utils:log_message("Broker CPU Usage - ~p | Broker Memory Usage - ~p", [AvgBrokerCpuUsage, AvgBrokerMemUsage]),
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
                  parse_and_store_cpu_usage(ResponseLines, NodeType),
                  parse_and_store_memory_usage(ResponseLines, NodeType)
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

parse_and_store_cpu_usage(NodeExporterResponseLines, NodeType) ->
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
                  ps_bench_store:record_cpu_usage(NodeType, CpuUsage)
      end.

parse_and_store_memory_usage(NodeExporterResponseLines, NodeType) ->
      MemUseLine = lists:filter(fun(Line) -> string:find(Line, ?MEM_USE_LINE_START) =/= nomatch andalso string:find(Line, ?HELP_LINE_START) =:= nomatch end, NodeExporterResponseLines),
      MemTotalLine = lists:filter(fun(Line) -> string:find(Line, ?MEM_TOTAL_LINE_START) =/= nomatch andalso string:find(Line, ?HELP_LINE_START) =:= nomatch end, NodeExporterResponseLines),

      MemUseValue = lists:foldl(fun(Line, Total) -> Total + parse_value_from_line(Line) end, 0, MemUseLine),
      MemTotalValue = lists:foldl(fun(Line, Total) -> Total + parse_value_from_line(Line) end, 0, MemTotalLine),
      MemUsagePct = (MemUseValue / MemTotalValue) * 100,
      
      % Store usage metric
      ps_bench_store:record_memory_usage(NodeType, MemUsagePct).

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

calculate_average_cpu_usage(NodeType) ->
      AllCpuUsageEvents = case NodeType of
                              local ->
                                    ps_bench_store:fetch_cpu_usage();
                              broker ->
                                    ps_bench_store:fetch_broker_cpu_usage()
                        end,

      TotalEvents = length(AllCpuUsageEvents),
      TotalUsage = lists:foldl(fun({_, UsageVal}, Total) -> Total + UsageVal end, 0, AllCpuUsageEvents),
      AvgUsage = TotalUsage / TotalEvents,
      AvgUsage.

calculate_average_memory_usage(NodeType) ->
      AllMemoryUsageEvents = case NodeType of
                              local ->
                                    ps_bench_store:fetch_memory_usage();
                              broker ->
                                    ps_bench_store:fetch_broker_memory_usage()
                        end,

      TotalEvents = length(AllMemoryUsageEvents),
      TotalUsage = lists:foldl(fun({_, UsageVal}, Total) -> Total + UsageVal end, 0, AllMemoryUsageEvents),
      AvgUsage = TotalUsage / TotalEvents,
      AvgUsage.