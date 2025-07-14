%%%-------------------------------------------------------------------
-module(metric_agg_srv).
-behaviour(gen_server).

-export([start_link/0, get_report/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(POLL_MS, 2000).

-record(state,
        {samples    = [],    %% latencies
         drops      = 0,     %% total drops
         volatility = 0,     %% total reconnects
         last_cnt   = 0,
         last_time  = 0,
         last_cpu   = 0}).

%%%===================================================================
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get_report() -> gen_server:call(?MODULE, report).

%%%===================================================================
init([]) ->
    application:ensure_all_started(os_mon),
    erlang:send_after(?POLL_MS, self(), poll),
    {ok, #state{last_time = now_ms()}}.

%%%-------------------------------------------------------------------
handle_info(poll,
            St=#state{samples = S0,
                      last_cnt = PrevCnt,
                      last_time = PrevTime}) ->
    lists:foreach(
      fun(P) -> P ! {metric_request, self()} end,
      pg:get_members(node_agents)),

    Now      = now_ms(),
    DeltaT   = max(1, Now - PrevTime),
    NewCnt   = length(S0) - PrevCnt,
    Tput     = NewCnt * 1000 div DeltaT,

    CpuRaw = cpu_sup:util(),
    Cpu    = case CpuRaw of
                N when is_number(N) -> N;
                {N, _}              -> N
             end,
    io:format("[metric] ~p msg/s   cpu=~.2f%%  drops=~p  vol=~p~n",
          [Tput, Cpu, St#state.drops, St#state.volatility]),

    erlang:send_after(?POLL_MS, self(), poll),
    {noreply, St#state{last_cnt  = length(S0),
                       last_time = Now,
                       last_cpu  = Cpu}};

%% reply from node_agent
handle_info({metric_reply, Lats, DropInc, VolInc},
            St=#state{samples = S, drops = D, volatility = V}) ->
    {noreply, St#state{samples    = Lats ++ S,
                       drops      = D + DropInc,
                       volatility = V + VolInc}};

handle_info(_, St) -> {noreply, St}.

%%%-------------------------------------------------------------------
%% synchronous report
handle_call(report, _From,
            St=#state{samples = S, drops = D, volatility = V,
                      last_cpu = Cpu}) ->
    case S of
        [] ->
            {reply, #{count=>0, drops=>D, volatility=>V, cpu=>Cpu}, St};
        _  ->
            Sorted = lists:sort(S),
            C      = length(Sorted),
            P = fun(F)->lists:nth(max(1,round(F*C)),Sorted) end,
            {reply,
             #{count=>C, p50=>P(0.5), p95=>P(0.95), p99=>P(0.99),
               cpu=>Cpu, drops=>D, volatility=>V},
             St}
    end.

handle_cast(_,St) -> {noreply,St}.
terminate(_,_)    -> ok.
code_change(_,S,_) -> {ok,S}.

%%%-------------------------------------------------------------------
now_ms() -> erlang:system_time(millisecond).
