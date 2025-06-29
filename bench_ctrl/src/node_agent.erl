%%%-------------------------------------------------------------------
%%% node_agent.erl
%%%-------------------------------------------------------------------
%%% • joins pg group ‘node_agents’
%%% • spawns workers/sub.py (via /bin/sh -c …) that prints
%%%     "timestamp_ns,lat_ms" lines
%%% • pushes latencies into ETS and replies to metric_agg_srv
%%%-------------------------------------------------------------------
-module(node_agent).
-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(ETS_OPTS, [ordered_set, private]).
-define(SUB_CMD, "/bin/sh -c 'BROKER=localhost:1884 ./workers/sub.py'").

-record(state,
        {tab        :: ets:tid(),
         sub_port   :: port()}).

%%%===================================================================
%%% API
%%%===================================================================
start_link() ->
    gen_server:start_link(?MODULE, [], []).

%%%===================================================================
%%% gen_server
%%%===================================================================
init([]) ->
    ensure_pg(),
    pg:join(node_agents, self()),

    Tab  = ets:new(lat_samples, ?ETS_OPTS),
    Port = open_port({spawn, ?SUB_CMD},
                     [stream, eof, exit_status, {line,256}, use_stdio]),

    io:format("~p node_agent up (subscriber ~p)~n", [node(), Port]),
    {ok, #state{tab = Tab, sub_port = Port}}.

%% no synchronous API for now
handle_call(_Req, _From, S) -> {reply, ok, S}.
handle_cast(_Msg, S)        -> {noreply, S}.

%%%-------------------------------------------------------------------
%%% info messages
%%%-------------------------------------------------------------------
%% 1 – metric poll
handle_info({metric_request, From},
            S = #state{tab = Tab}) ->
    Lats = [Lat || {_K,Lat} <- ets:tab2list(Tab)],
    ets:delete_all_objects(Tab),
    From ! {metric_reply, Lats},
    {noreply, S};

%% 2 – CSV line from Python: Port delivers {data,{eol,Bin}}
handle_info({Port,{data,{eol,RawLine}}},
            S = #state{tab = Tab, sub_port = Port}) ->
    %% convert to plain string safely
    Line =
        case RawLine of
            Bin when is_binary(Bin) -> binary_to_list(Bin);
            Str when is_list(Str)   -> Str
        end,

    case string:tokens(Line, ",") of
        [_,LatStr] ->
            ets:insert(Tab,
                       {erlang:unique_integer([monotonic]),
                        list_to_float(LatStr)});
        _ -> ok
    end,
    {noreply, S};


%% 3 – subprocess exit
handle_info({'EXIT',Port,_Reason},
            S = #state{sub_port = Port}) ->
    io:format("subscriber exited (~p)~n",[Port]),
    {noreply, S};

handle_info(_, S) -> {noreply, S}.

%%%-------------------------------------------------------------------
terminate(_Why, #state{sub_port = Port}) ->
    catch port_close(Port),
    ok.

code_change(_, S, _) -> {ok, S}.

%%%-------------------------------------------------------------------
ensure_pg() ->
    case whereis(pg) of
        undefined -> pg:start_link();
        _         -> ok
    end.
