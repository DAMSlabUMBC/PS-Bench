%%%-------------------------------------------------------------------
%% @doc bench_ctrl public API
%% @end
%%%-------------------------------------------------------------------

-module(ps_bench_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->

    % Load current module name
    {ok, NodeName} = application:get_env(node_name),

    {ok, TopSupPid} = ps_bench_sup:start_link(NodeName),
    ps_bench_sup:start_benchmark(),
    {ok, TopSupPid}.

stop(_State) ->
    ok.

%% internal functions
