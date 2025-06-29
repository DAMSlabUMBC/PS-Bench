%%%-------------------------------------------------------------------
%% @doc bench_ctrl public API
%% @end
%%%-------------------------------------------------------------------

-module(bench_ctrl_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    bench_ctrl_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
