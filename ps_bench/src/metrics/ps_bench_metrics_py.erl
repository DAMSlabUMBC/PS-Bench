-module(ps_bench_metrics_py).
-behaviour(gen_server).
-export([start_link/1, ingest_window/3]).
-export([init/1, handle_cast/2, handle_call/3, terminate/2, code_change/3]).
-define(DEFAULT_OUT_DIR, "/app/out").

start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Opts, []).

ingest_window(RunId, WinStartMs, WinMap) ->
    gen_server:cast(?MODULE, {ingest, RunId, WinStartMs, WinMap}).

get_out_dir() ->
    case os:getenv("METRICS_DIR") of
        false -> ?DEFAULT_OUT_DIR;
        Dir   -> Dir
    end.

init(Opts) ->
    %% Build an absolute path to priv/py_engine that works in dev & release
    PrivDir = code:priv_dir(ps_bench),
    PyPath  = filename:join(PrivDir, "py_engine"),

    {ok, Plugins} = ps_bench_config_manager:fetch_python_metric_plugins(),
    Listener = proplists:get_value(listener_name, Opts, ps_bench_metrics_listener),

    OutDir0 = get_out_dir(),
    OutDir  = unicode:characters_to_binary(OutDir0),   %% send a binary, not a list
    ok = filelib:ensure_dir(filename:join(OutDir0, "dummy")),

    {ok, Py} = python:start_link([{python_path, [PyPath]},
                                {python, "python3"}]),
    ok = python:call(Py, window_engine, start, [Listener, Plugins, OutDir]),

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