-module(ps_bench_purpose_store).
-export([init/0, store_purpose_mapping/1, get_message_purpose/1,
         get_subscription_purpose/1, get_expected_sources/1,
         get_all_subscribers/0, clear/0]).

-define(PURPOSE_TABLE, ps_bench_purpose_mappings).

%% Create/reset the ETS table for purpose storage
init() ->
    case ets:info(?PURPOSE_TABLE) of
        undefined ->
            ets:new(?PURPOSE_TABLE, [named_table, public, set]),
            ok;
        _ ->
            ets:delete_all_objects(?PURPOSE_TABLE),
            ok
    end.

%% Load purpose mappings from scenario config
%% Takes list like [{client_name, [{message_purpose, "..."}]}]
store_purpose_mapping(PurposeMappingList) when is_list(PurposeMappingList) ->
    lists:foreach(fun({ClientName, Properties}) ->
        ClientNameBin = to_binary(ClientName),

        % Store MP if present
        case proplists:get_value(message_purpose, Properties) of
            undefined -> ok;
            MP -> ets:insert(?PURPOSE_TABLE, {{mp, ClientNameBin}, to_binary(MP)})
        end,

        % Store SP if present
        case proplists:get_value(subscription_purpose, Properties) of
            undefined -> ok;
            SP -> ets:insert(?PURPOSE_TABLE, {{sp, ClientNameBin}, to_binary(SP)})
        end,

        % Store expected sources if present
        case proplists:get_value(expected_data_sources, Properties) of
            undefined -> ok;
            Sources ->
                SourcesBin = [to_binary(S) || S <- Sources],
                ets:insert(?PURPOSE_TABLE, {{sources, ClientNameBin}, SourcesBin})
        end
    end, PurposeMappingList),
    ok.

get_message_purpose(ClientName) ->
    ClientNameBin = to_binary(ClientName),
    case ets:lookup(?PURPOSE_TABLE, {mp, ClientNameBin}) of
        [{{mp, _}, MP}] -> {ok, MP};
        [] -> {error, not_found}
    end.

get_subscription_purpose(ClientName) ->
    ClientNameBin = to_binary(ClientName),
    case ets:lookup(?PURPOSE_TABLE, {sp, ClientNameBin}) of
        [{{sp, _}, SP}] -> {ok, SP};
        [] -> {error, not_found}
    end.

get_expected_sources(ClientName) ->
    ClientNameBin = to_binary(ClientName),
    case ets:lookup(?PURPOSE_TABLE, {sources, ClientNameBin}) of
        [{{sources, _}, Sources}] -> {ok, Sources};
        [] -> {error, not_found}
    end.

%% Get all clients with subscription purposes
get_all_subscribers() ->
    Pattern = {{sp, '$1'}, '_'},
    Subscribers = ets:match(?PURPOSE_TABLE, Pattern),
    [Sub || [Sub] <- Subscribers].

clear() ->
    case ets:info(?PURPOSE_TABLE) of
        undefined -> ok;
        _ -> ets:delete_all_objects(?PURPOSE_TABLE)
    end.

%% Helper to normalize everything to binary
to_binary(Value) when is_binary(Value) -> Value;
to_binary(Value) when is_list(Value) -> list_to_binary(Value);
to_binary(Value) when is_atom(Value) -> atom_to_binary(Value, utf8);
to_binary(Value) -> Value.
