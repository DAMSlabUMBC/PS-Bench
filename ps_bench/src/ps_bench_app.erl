%%%-------------------------------------------------------------------
%% @doc bench_ctrl public API
%% @end
%%%-------------------------------------------------------------------

-module(ps_bench_app).

-behaviour(application).

-include("ps_bench_config.hrl").

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->

    io:format("=== Loading Config ===~n"),
    case ps_bench_config_manager:load_config_from_env_vars() of
        ok ->
            io:format("=== Initializing Benchmark ===~n"),
            {ok, NodeName} = ps_bench_config_manager:fetch_node_name(),
            io:format("~p starting~n", [NodeName]),

            {ok, Value} = ps_bench_config_manager:fetch_property_for_device(occupancy_sensor, ?DEVICE_SIZE_MEAN_PROP),
            io:format("Got value: ~p~n", [Value]),

            {ok, NodeList} = ps_bench_config_manager:fetch_node_list(),

            {ok, TopSupPid} = ps_bench_sup:start_link(NodeName, NodeList),
            ps_bench_sup:start_benchmark(NodeName),
            {ok, TopSupPid};
        {error, Reason} ->
            {error, Reason}
    end.
    

stop(_State) ->
    ok.

%% internal functions
