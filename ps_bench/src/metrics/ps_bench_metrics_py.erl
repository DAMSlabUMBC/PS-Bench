-module(ps_bench_metrics_py).
-behaviour(gen_server).
-export([start_link/1, ingest_window/3]).
-export([init/1, handle_cast/2, handle_call/3, terminate/2, code_change/3]).

-record(state, {py}).

start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Opts, []).

ingest_window(RunId, WinStartMs, WinMap) ->
    gen_server:cast(?MODULE, {ingest, RunId, WinStartMs, WinMap}).

init(Opts) ->
    Listener = maps:get(listener_name, Opts, ps_bench_metrics_listener),
    {ok, PyPath} = ps_bench_config_manager:fetch_python_metric_engine_path(),
    {ok, Py} = python:start([{python_path, [PyPath]}]),
    {ok, Plugins}  = ps_bench_config_manager:fetch_metric_plugin_list(),
    ok = python:call(Py, window_engine, start, [Listener, Plugins]),
    {ok, #state{py=Py}}.

handle_cast({ingest, RunId, WinStartMs, WinMap}, S=#state{py=Py}) ->
    ok = python:call(Py, window_engine, ingest_window, [RunId, WinStartMs, WinMap]),
    {noreply, S};
handle_cast(_,S)->{noreply,S}.

handle_call(_,_,S)->{reply,ok,S}.
terminate(_, #state{py=Py}) -> catch python:stop(Py), ok.
code_change(_,S,_) -> {ok,S}.
