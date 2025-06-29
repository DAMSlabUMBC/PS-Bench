-module(dynamic_sup).
-behaviour(supervisor).
-export([start_link/1, start_child/1, init/1]).

start_link(ChildMod) ->
    supervisor:start_link({local, dynamic_sup}, ?MODULE, ChildMod).

start_child(Sup) ->
    supervisor:start_child(Sup, []).

init(ChildMod) ->
    Template = #{id => ChildMod,
                 start => {ChildMod, start_link, []},
                 restart => transient, shutdown => 4000,
                 type => worker, modules => [ChildMod]},
    {ok, {{simple_one_for_one, 10, 60}, [Template]}}.
