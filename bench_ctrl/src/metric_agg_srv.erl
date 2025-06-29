%%%-------------------------------------------------------------------
-module(metric_agg_srv).
-behaviour(gen_server).

-export([start_link/0, get_report/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(POLL_MS, 2000).

-record(state,
        {samples   = []  :: [float()],
         last_cnt  = 0,
         last_time = 0,
         last_cpu  = 0}).       %% most recent cpu %

%%%===================================================================
%%% API
%%%===================================================================
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get_report() ->
    gen_server:call(?MODULE, report).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([]) ->
    application:ensure_all_started(os_mon),
    erlang:send_after(?POLL_MS, self(), poll),
    {ok, #state{last_time = now_ms()}}.

%%---------------- poll timer ----------------------------------------
handle_info(poll,
            St=#state{samples = Samples0,
                      last_cnt = PrevCnt,
                      last_time = PrevTime}) ->
    lists:foreach(
      fun(P) -> P ! {metric_request, self()} end,
      pg:get_members(node_agents, all)),

    Now      = now_ms(),
    DeltaT   = max(1, Now - PrevTime),
    NewCount = length(Samples0) - PrevCnt,
    Tput     = NewCount * 1000 div DeltaT,

    CpuRaw = cpu_sup:util(),
    Cpu    = case CpuRaw of
            N when is_number(N) -> N;
            {N, _}             -> N          % in case another OS returns a tuple
         end,
    io:format("[metric] ~p msg/s   cpu=~p%~n", [Tput, Cpu]),

    erlang:send_after(?POLL_MS, self(), poll),
    {noreply, St#state{last_cnt = length(Samples0),
                       last_time = Now,
                       last_cpu  = Cpu}};

%%---------------- node_agent reply ----------------------------------
handle_info({metric_reply, List}, St=#state{samples = All}) ->
    {noreply, St#state{samples = List ++ All}};

handle_info(_, St) ->
    {noreply, St}.

%%---------------- synchronous report --------------------------------
handle_call(report, _From, St=#state{samples = []}) ->
    {reply, #{count => 0}, St};

handle_call(report, _From,
            St=#state{samples = All, last_cpu = Cpu}) ->
    Sorted = lists:sort(All),
    C      = length(Sorted),
    %% local fun for percentile lookup
    PFun   = fun(F) -> lists:nth(max(1, round(F*C)), Sorted) end,
    Report = #{count => C,
               p50   => PFun(0.50),
               p95   => PFun(0.95),
               p99   => PFun(0.99),
               cpu   => Cpu},
    {reply, Report, St}.

%%-- empty cast handler just to satisfy behaviour --------------------
handle_cast(_Msg, St) -> {noreply, St}.

terminate(_,_)  -> ok.
code_change(_,St,_) -> {ok,St}.

%%%-------------------------------------------------------------------
now_ms() -> erlang:system_time(millisecond).
