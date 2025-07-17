-module(ps_bench_client).
-behaviour(gen_server).

%% public
-export([start_link/2]).

%% gen_server callbacks
-export([init/1, handle_call/3,
         handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

start_link(ClientName, TestName) ->
    gen_server:start_link(?MODULE, [{ClientName, TestName}], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([{ClientName, TestName}]) ->
    % Fetch required config options
    {ok, InterfaceName} = application:get_env(client_interface_name),
    {ok, InterfaceType} = application:get_env(client_interface_type),

    % Spawn the interface
    io:format("Client ~s spawned interface ~s with type ~s~n", [ClientName, InterfaceName, InterfaceType]),

    {ok, {ClientName, TestName}}.

handle_call(_Msg, _From, State) ->
    %% no call logic yet, just fulfill behaviour
    {noreply, State}.

handle_cast(_Msg, State) ->
    %% no cast logic yet, just fulfill behaviour
    {noreply, State}.

handle_info(_Info, State) ->
    %% no info logic yet, just fulfill behaviour
    {noreply, State}.

terminate(normal, {ClientName, TestName}) -> 
    io:format("Client Shutdown ~s for ~s~n",[ClientName, TestName]),
    ok;

terminate(shutdown, {ClientName, TestName}) ->
    io:format("Supervisor Shutdown ~s for ~s~n",[ClientName, TestName]),
    ok.

code_change(_OldVsn, State, _Extra) -> 
    {ok, State}.