-module(ps_bench_purpose_store).
-export([init/0, store_purpose_mapping/1, get_message_purpose/1,
         get_subscription_purpose/1, get_expected_sources/1,
         get_message_purpose_with_fallback/1,
         get_subscription_purpose_with_fallback/1,
         get_expected_sources_with_fallback/1,
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
    io:format("~n[PURPOSE-STORE] Storing ~p purpose mappings~n", [length(PurposeMappingList)]),
    lists:foreach(fun({ClientName, Properties}) ->
        ClientNameBin = to_binary(ClientName),
        io:format("~n[PURPOSE-STORE] Processing client: ~p (binary: ~p)~n", [ClientName, ClientNameBin]),

        % Store MP if present
        case proplists:get_value(message_purpose, Properties) of
            undefined ->
                io:format("~n[PURPOSE-STORE]   No MP~n"),
                ok;
            MP ->
                MPBin = to_binary(MP),
                io:format("~n[PURPOSE-STORE]   MP: ~p~n", [MPBin]),
                ets:insert(?PURPOSE_TABLE, {{mp, ClientNameBin}, MPBin})
        end,

        % Store SP if present
        case proplists:get_value(subscription_purpose, Properties) of
            undefined ->
                io:format("~n[PURPOSE-STORE]   No SP~n"),
                ok;
            SP ->
                SPBin = to_binary(SP),
                io:format("~n[PURPOSE-STORE]   SP: ~p~n", [SPBin]),
                ets:insert(?PURPOSE_TABLE, {{sp, ClientNameBin}, SPBin})
        end,

        % Store expected sources if present
        case proplists:get_value(expected_data_sources, Properties) of
            undefined ->
                ok;
            Sources ->
                SourcesBin = [to_binary(S) || S <- Sources],
                io:format("~n[PURPOSE-STORE]   Sources: ~p~n", [SourcesBin]),
                ets:insert(?PURPOSE_TABLE, {{sources, ClientNameBin}, SourcesBin})
        end
    end, PurposeMappingList),
    io:format("~n[PURPOSE-STORE] Done storing purpose mappings~n"),
    ok.

get_message_purpose(ClientName) ->
    case ets:info(?PURPOSE_TABLE) of
        undefined ->
            {error, not_found};
        _ ->
            ClientNameBin = to_binary(ClientName),
            case ets:lookup(?PURPOSE_TABLE, {mp, ClientNameBin}) of
                [{{mp, _}, MP}] -> {ok, MP};
                [] -> {error, not_found}
            end
    end.

get_subscription_purpose(ClientName) ->
    case ets:info(?PURPOSE_TABLE) of
        undefined ->
            {error, not_found};
        _ ->
            ClientNameBin = to_binary(ClientName),
            case ets:lookup(?PURPOSE_TABLE, {sp, ClientNameBin}) of
                [{{sp, _}, SP}] -> {ok, SP};
                [] -> {error, not_found}
            end
    end.

get_expected_sources(ClientName) ->
    case ets:info(?PURPOSE_TABLE) of
        undefined ->
            {error, not_found};
        _ ->
            ClientNameBin = to_binary(ClientName),
            case ets:lookup(?PURPOSE_TABLE, {sources, ClientNameBin}) of
                [{{sources, _}, Sources}] -> {ok, Sources};
                [] -> {error, not_found}
            end
    end.

%% Get all clients with subscription purposes
get_all_subscribers() ->
    case ets:info(?PURPOSE_TABLE) of
        undefined ->
            [];
        _ ->
            Pattern = {{sp, '$1'}, '_'},
            Subscribers = ets:match(?PURPOSE_TABLE, Pattern),
            [Sub || [Sub] <- Subscribers]
    end.

clear() ->
    case ets:info(?PURPOSE_TABLE) of
        undefined -> ok;
        _ -> ets:delete_all_objects(?PURPOSE_TABLE)
    end.

%% ============================================================================
%% Fallback lookup functions - try multiple strategies to find purposes
%% ============================================================================

%% Get MP with fallback: tries exact name -> simplified name -> device type
get_message_purpose_with_fallback(ClientName) ->
    ClientNameBin = to_binary(ClientName),
    case get_message_purpose(ClientNameBin) of
        {ok, MP} ->
            {ok, MP};
        {error, not_found} ->
            % Try simplified name (e.g., "runner1_factory_sensor_1" -> "factory_sensor_01")
            SimplifiedName = extract_simplified_client_name(ClientNameBin),
            case get_message_purpose(SimplifiedName) of
                {ok, MP} ->
                    {ok, MP};
                {error, not_found} ->
                    % Try device type (e.g., "runner1_factory_sensor_1" -> "factory_sensor")
                    DeviceType = extract_device_type(ClientNameBin),
                    get_message_purpose(DeviceType)
            end
    end.

%% Get SP with fallback: tries exact name -> simplified name -> device type
get_subscription_purpose_with_fallback(ClientName) ->
    ClientNameBin = to_binary(ClientName),
    case get_subscription_purpose(ClientNameBin) of
        {ok, SP} ->
            {ok, SP};
        {error, not_found} ->
            SimplifiedName = extract_simplified_client_name(ClientNameBin),
            case get_subscription_purpose(SimplifiedName) of
                {ok, SP} ->
                    {ok, SP};
                {error, not_found} ->
                    DeviceType = extract_device_type(ClientNameBin),
                    get_subscription_purpose(DeviceType)
            end
    end.

%% Get expected sources with fallback: tries exact name -> simplified name -> device type
get_expected_sources_with_fallback(ClientName) ->
    ClientNameBin = to_binary(ClientName),
    case get_expected_sources(ClientNameBin) of
        {ok, Sources} ->
            {ok, Sources};
        {error, not_found} ->
            SimplifiedName = extract_simplified_client_name(ClientNameBin),
            case get_expected_sources(SimplifiedName) of
                {ok, Sources} ->
                    {ok, Sources};
                {error, not_found} ->
                    DeviceType = extract_device_type(ClientNameBin),
                    get_expected_sources(DeviceType)
            end
    end.

%% ============================================================================
%% Helper functions for name extraction
%% ============================================================================

%% Extract device type from full client name
%% "runner1_factory_sensor_1" -> "factory_sensor"
%% "runner1_subscriber_2" -> "subscriber"
extract_device_type(ClientNameBin) when is_binary(ClientNameBin) ->
    ClientNameStr = binary_to_list(ClientNameBin),
    Parts = string:split(ClientNameStr, "_", all),
    case Parts of
        [_NodeName | DeviceParts] ->
            % Remove the numeric suffix if present
            case lists:reverse(DeviceParts) of
                [LastPart | RestReversed] ->
                    case string:to_integer(LastPart) of
                        {_Index, ""} ->
                            % Last part is a number, remove it
                            DeviceType = string:join(lists:reverse(RestReversed), "_"),
                            list_to_binary(DeviceType);
                        _ ->
                            % Last part is not a number, keep all device parts
                            list_to_binary(string:join(DeviceParts, "_"))
                    end;
                _ ->
                    ClientNameBin
            end;
        _ ->
            ClientNameBin
    end;
extract_device_type(ClientName) ->
    extract_device_type(to_binary(ClientName)).

%% Strip node prefix and zero-pad index
%% "runner1_factory_sensor_1" -> "factory_sensor_01"
extract_simplified_client_name(ClientNameBin) when is_binary(ClientNameBin) ->
    ClientNameStr = binary_to_list(ClientNameBin),
    Parts = string:split(ClientNameStr, "_", all),
    case Parts of
        [_NodeName | DeviceParts] ->
            case lists:reverse(DeviceParts) of
                [IndexStr | RestReversed] ->
                    case string:to_integer(IndexStr) of
                        {Index, ""} when Index < 10 ->
                            % Zero-pad single digits
                            PaddedIndex = lists:flatten(io_lib:format("~2.10.0B", [Index])),
                            Reconstructed = string:join(lists:reverse(RestReversed) ++ [PaddedIndex], "_"),
                            list_to_binary(Reconstructed);
                        _ ->
                            list_to_binary(string:join(DeviceParts, "_"))
                    end;
                _ ->
                    ClientNameBin
            end;
        _ ->
            ClientNameBin
    end;
extract_simplified_client_name(ClientName) ->
    extract_simplified_client_name(to_binary(ClientName)).

%% Helper to normalize everything to binary
to_binary(Value) when is_binary(Value) -> Value;
to_binary(Value) when is_list(Value) -> list_to_binary(Value);
to_binary(Value) when is_atom(Value) -> atom_to_binary(Value, utf8);
to_binary(Value) -> Value.
