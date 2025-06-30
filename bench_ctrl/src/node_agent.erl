%%%-------------------------------------------------------------------
%%% node_agent
%%% • Spawns the Python MQTT subscriber in a port.
%%% • Collects latency samples in an ETS table (ordered_set).
%%% • Replies to {metric_request, Pid} with {metric_reply, [Latencies]}.
%%%-------------------------------------------------------------------
-module(node_agent).

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {tab,          %% ETS table id
                port}).        %% Port talking to sub.py

%%%===================================================================
%%% Public API
%%%===================================================================

start_link() ->
    gen_server:start_link(?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    %% allow process‑group look‑ups
    pg:join(node_agents, self()),

    Cmd = "/bin/sh -c 'BROKER=localhost:1884 "
          "TOPIC=bench/test exec ./workers/sub.py'",

    %% open port – line‑oriented, huge max line
    Port = open_port({spawn, Cmd},
                     [stream, eof, exit_status, {line, 32768}]),

    Tab  = ets:new(lat_samples, [ordered_set, private]),
    {ok, #state{tab = Tab, port = Port}}.

%%--------------------------------------------------------------------
handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% metric aggregation request
handle_info({metric_request, From},
            #state{tab = Tab}=St) ->
    Lats = [Lat || {_, Lat} <- ets:tab2list(Tab)],
    ets:delete_all_objects(Tab),
    From ! {metric_reply, Lats},
    {noreply, St};

%%--------------------------------------------------------------------
%% FIRST – the “{eol, Line}” variant
handle_info({Port, {data, {eol, Line}}},
            #state{tab = Tab, port = Port}=St) ->
    maybe_store_line(Line, Tab),
    {noreply, St};

%% THEN – plain line variant (list/binary)
handle_info({Port, {data, Line}},
            #state{tab = Tab, port = Port}=St)
  when is_list(Line); is_binary(Line) ->
    maybe_store_line(Line, Tab),
    {noreply, St};

%%--------------------------------------------------------------------
%% everything else
handle_info(Other, St) ->
    logger:debug("unhandled message: ~p", [Other]),
    {noreply, St}.

terminate(_Reason, _State) -> ok.

code_change(_Vsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal helpers
%%%===================================================================

maybe_store_line(Line, Tab) ->
    %% Line already has no line‑feed.
    case string:tokens(Line, ",") of
        [_Stamp, LatStr] ->
            case string:to_float(LatStr) of
                {Lat, _} ->
                    K = erlang:unique_integer([monotonic]),
                    ets:insert(Tab, {K, Lat}),
                    ok;
                _ ->
                    logger:warning("cannot convert latency: ~p", [Line])
            end;
        _ ->
            logger:warning("malformed line: ~p", [Line])
    end.
