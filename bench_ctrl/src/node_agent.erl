%%%-------------------------------------------------------------------
%%% node_agent – collects latency + fault‑tolerance metrics
%%%-------------------------------------------------------------------
-module(node_agent).
-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-record(state,
        {tab,            %% ETS id for latencies
         port,           %% Port to sub.py
         last_seq   = -1,
         drops      = 0, %% Δ dropped msgs since last poll
         volatility = 0  %% Δ disconnects since last poll
        }).

%%%===================================================================
start_link() ->
    gen_server:start_link(?MODULE, [], []).

%%%===================================================================
init([]) ->
    pg:join(node_agents, self()),

    {ok, Broker} = application:get_env(broker),
    {ok, BrokerNetPort} = application:get_env(port),

    Cmd  = "/bin/sh -c 'BROKER=" ++ Broker ++":" ++ BrokerNetPort ++ 
           " TOPIC=bench/test exec ./workers/sub.py'",
    Port = open_port({spawn, Cmd},
                     [stream, eof, exit_status, {line, 32768}]),

    process_flag(trap_exit, true),       %% so we see {'EXIT',Port,…}
    Tab  = ets:new(lat_samples, [ordered_set, private]),
    {ok, #state{tab = Tab, port = Port}}.

%%%-------------------------------------------------------------------
handle_call(_Req, _From, St) ->
    {reply, ok, St}.

handle_cast(_, St) -> {noreply, St}.

%%%-------------------------------------------------------------------
%% poll from metric_agg_srv
handle_info({metric_request, From},
            St=#state{tab = Tab, drops = D, volatility = V}) ->
    Lats = [Lat || {_,Lat} <- ets:tab2list(Tab)],
    ets:delete_all_objects(Tab),
    From ! {metric_reply, Lats, D, V},
    {noreply, St#state{drops = 0, volatility = 0}};  %% reset deltas

%%%-------------------------------------------------------------------
%% line‑oriented port data  – variant 1
handle_info({Port, {data, {eol, Line}}},
            St=#state{port = Port}) ->
    {noreply, maybe_store_line(Line, St)};

%% variant 2 – plain list / binary
handle_info({Port, {data, Line}},
            St=#state{port = Port})
  when is_list(Line); is_binary(Line) ->
    {noreply, maybe_store_line(Line, St)};

%% port closed → volatility +1
handle_info({Port, {exit_status, _Code}},
            St=#state{port = Port, volatility = V}) ->
    {noreply, St#state{volatility = V + 1}};

handle_info({'EXIT', Port, _Reason},
            St=#state{port = Port, volatility = V}) ->
    {noreply, St#state{volatility = V + 1}};

%% everything else
handle_info(Other, St) ->
    logger:debug("unhandled: ~p",[Other]),
    {noreply, St}.

terminate(_,_)  -> ok.
code_change(_,St,_) -> {ok,St}.

%%%-------------------------------------------------------------------
%%% helper
%%%-------------------------------------------------------------------
maybe_store_line(Line,
                 St=#state{tab = Tab,
                           last_seq = Last,
                           drops    = D0}) ->
    case string:tokens(Line, ",") of
        [SeqS, LatS] ->
            {Seq, _} = string:to_integer(SeqS),
            DropInc  = if Last == -1 -> 0;
                          true       -> max(0, Seq - Last - 1)
                       end,
            case string:to_float(LatS) of
                {Lat, _} ->
                    K = erlang:unique_integer([monotonic]),
                    ets:insert(Tab, {K, Lat}),
                    St#state{last_seq = Seq,
                             drops    = D0 + DropInc};
                _ ->
                    logger:warning("bad latency: ~p",[Line]),
                    St
            end;
        _ ->
            logger:warning("malformed: ~p",[Line]),
            St
    end.
