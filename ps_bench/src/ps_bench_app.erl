%%%-------------------------------------------------------------------
%% @doc bench_ctrl public API
%% @end
%%%-------------------------------------------------------------------

-module(ps_bench_app).

-behaviour(application).

-include("ps_bench_config.hrl").

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->

    % Load current module name
    {ok, NodeName} = application:get_env(node_name),
    {ok, NodeList} = application:get_env(node_list),
    {ok, TestConfigFile} = application:get_env(test_configuration_file),
    {ok, DeviceDefsDir} = application:get_env(device_definitions_directory),

    case ps_bench_config_manager:load_config(DeviceDefsDir, TestConfigFile) of
        ok ->

            {ok, Value} = ps_bench_config_manager:fetch_property_for_device(occupancy_sensor, ?DEVICE_SIZE_MEAN_FIELD),
            io:format("Got value: ~p~n", [Value]),

            %{ok, TopSupPid} = ps_bench_sup:start_link(NodeName, NodeList),
            %ps_bench_sup:start_benchmark(NodeName),
            %{ok, TopSupPid}.
            {ok, self()};
        {error, Reason} ->
            {error, Reason}
    end.
    

stop(_State) ->
    ok.

%% internal functions
