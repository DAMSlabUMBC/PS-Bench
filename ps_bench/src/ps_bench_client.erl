-module(ps_bench_client).
-behaviour(gen_server).

%% public
-export([start_link/2]).

%% gen_server callbacks
-export([init/1, handle_call/3,
         handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

start_link(TestName, ClientName) ->
    gen_server:start_link(?MODULE, [{TestName, ClientName}], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([{TestName, ClientName}]) ->
    % Fetch required config options
    {ok, InterfaceName} = application:get_env(client_interface_name),
    {ok, InterfaceType} = application:get_env(client_interface_type),
 
    % Spawn the interface
    io:format("Client ~s spawned interface ~s with type ~s for test ~s~n", [ClientName, InterfaceName, InterfaceType, TestName]),

    {ok, {TestName, ClientName}}.

handle_call(_Msg, _From, State) ->
    %% no call logic yet, just fulfill behaviour
    {noreply, State}.

handle_cast(_Msg, State) ->
    %% no cast logic yet, just fulfill behaviour
    {noreply, State}.

handle_info(_Info, State) ->
    %% no info logic yet, just fulfill behaviour
    {noreply, State}.

terminate(normal, {TestName, ClientName}) -> 
    io:format("Client Shutdown ~s for ~s~n",[TestName, ClientName]),
    ok;

terminate(shutdown, {TestName, ClientName}) ->
    io:format("Supervisor Shutdown ~s for ~s~n",[TestName, ClientName]),
    ok.

code_change(_OldVsn, State, _Extra) -> 
    {ok, State}.