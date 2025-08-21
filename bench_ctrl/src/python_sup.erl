-behaviour(gen_server).
-export([start_link/1]).     %% FunList = [{Name, MFA}, …]

start_link(Funs) ->
    gen_server:start_link(?MODULE, Funs, []).

init(Funs) ->
    %% Spin up a single Python process
    {ok,Exe} = filename:join(code:priv_dir(bench_ctrl),"py/engine.py"),
    Port = open_port({spawn_executable, Exe},
                     [exit_status, use_stdio, binary, {packet,4}]),

    %% Tell the engine which functions we expect it to expose
    lists:foreach(fun ({Name,_}) ->
                        send_py(Port,{load,Name})
                  end, Funs),
    {ok, #{port=>Port, funs=>maps:from_list(Funs)}}.

%%% Synchronous F = {call, FunName, Args}
handle_call({call, Name, Args}, _From, S=#{port:=P, funs:=Funs}) ->
    case maps:get(Name,Funs,undefined) of
        undefined   -> {reply,{error,unknown_fun},S};
        _MFA ->
            Ref = make_ref(),
            send_py(P,{exec,Ref,Name,Args}),
            receive
                {py,Ref,Result} -> {reply,{ok,Result},S}
            after 5000 ->
                {reply,timeout,S}
            end
    end.

send_py(Port,Term) ->
    Port ! {self(),{command,term_to_binary(Term)}}.
