%%%-------------------------------------------------------------------
%%% clock_sync.erl  –– send a tick every second, keep offset map
%%%-------------------------------------------------------------------
-module(clock_sync).
-behaviour(gen_server).

%% public
-export([start_link/0, offset/1]).

%% gen_server callbacks
-export([init/1, handle_call/3,
         handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(INTVL, 1000).  %% milliseconds

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% ask for current offset to Node
offset(Node) ->
    gen_server:call(?MODULE, {get_offset, Node}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    %% ETS table for offsets {Node, Microsecs}
    Tab = ets:new(clock_sync_offsets, [named_table, public, set]),
     erlang:send_after(?INTVL, self(), tick),
    {ok, #{table => Tab}}.

handle_call({get_offset, Node}, _From, S = #{table := Tab}) ->
    case ets:lookup(Tab, Node) of
        [{Node, Off}] -> {reply, Off, S};
        []            -> {reply, undefined, S}
    end;
handle_call(_Req, _From, S) ->
    {reply, ok, S}.

handle_cast(_Msg, S) ->
    %% no cast logic yet, just fulfill behaviour
    {noreply, S}.

handle_info(tick, S = #{table := _}) ->
    lists:foreach(fun ping/1, nodes()),
    erlang:send_after(?INTVL, self(), tick),
    {noreply, S};

handle_info(_Info, S) ->
    {noreply, S}.

terminate(_Reason, _S) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%%-------------------------------------------------------------------
%%% Internal helpers
%%%-------------------------------------------------------------------
ping(Node) ->
    case rpc:call(Node, erlang, system_time, [microsecond]) of
        {badrpc, _} -> ok;
        RemoteNow   ->
            LocalNow = erlang:system_time(microsecond),
            Offset   = RemoteNow - LocalNow,
            ets:insert(clock_sync_offsets, {Node, Offset})
    end.
