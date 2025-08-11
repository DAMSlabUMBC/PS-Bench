-module(ps_bench_metrics_listener).
-behaviour(gen_server).
-export([start_link/0]).
-export([init/1, handle_info/2, handle_cast/2, handle_call/3]).

start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).
init([]) -> process_flag(trap_exit, true), {ok, #{}}.

%% Python -> Erlang summaries
handle_info({win_summary, RunId, WinStartMs, Summary}, State) ->
    ps_bench_store:put_window_summary(RunId, WinStartMs, Summary),
    {noreply, State};
handle_info(_, State) -> {noreply, State}.

handle_cast(_, State) -> {noreply, State}.
handle_call(_, _From, State) -> {reply, ok, State}.
