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

    {ok, PyPath} = ps_bench_config_manager:fetch_python_metric_engine_path(),
    {ok, Plugins}  = ps_bench_config_manager:fetch_python_metric_plugins(),
    Listener = proplists:get_value(listener_name, Opts, ps_bench_metrics_listener),
    
    % Initialize the python interface nad load the plugins
    {ok, Py} = python:start([{python_path, [PyPath]}, {python, "python3"}]),
    ok = python:call(Py, window_engine, start, [Listener, Plugins]),

    {ok, #{py => Py}}.

handle_cast({ingest, RunId, WinStartMs, WinMap}, State = #{py := Py}) ->
    ok = python:call(Py, window_engine, ingest_window, [RunId, WinStartMs, WinMap]),
    {noreply, State};

handle_cast(_, State) ->
    {noreply,State}.

handle_call(_, _, State) -> 
    {reply,ok,State}.

terminate(_, _State = #{py := Py}) -> 
    catch python:stop(Py), 
    ok.

code_change(_, State, _) -> 
    {ok, State}.