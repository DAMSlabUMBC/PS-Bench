-module(ps_bench_pbac_correctness_plugin).
-export([init/1, calc/0, load_purpose_mappings/1]).

-record(state, {
    output_dir :: string()  % Where to write results
}).

%% Store plugin state in persistent_term (fast global storage)
-define(STATE_KEY, {?MODULE, state}).

%%%===================================================================
%%% Plugin callbacks (PS-Bench calls these)
%%%===================================================================

%% Initialize the plugin
%% PS-Bench calls this once at the start of the test
init(OutDir) ->
    State = #state{output_dir = OutDir},
    persistent_term:put(?STATE_KEY, State),  % Save state globally

    % Initialize the purpose mapping store
    ok = ps_bench_purpose_store:init(),

    % Try to load purpose mappings from scenario configuration
    % This will be populated by the test framework when scenario is loaded
    io:format("PBAC Correctness Plugin initialized. Output: ~s~n", [OutDir]),
    io:format("Purpose store initialized. Waiting for purpose_mapping configuration...~n"),
    ok.

%% Calculate metrics
%% PS-Bench calls this once at the END of the test
%% This is where we crunch all the numbers to see if PBAC worked!
calc() ->
    State = persistent_term:get(?STATE_KEY),
    OutDir = State#state.output_dir,

    io:format("Calculating PBAC correctness metrics...~n"),

    % Fetch all the events that happened during the test
    % PublishEvents: Every time someone published a message
    % RecvEvents: Every time someone received a message
    PublishEvents = ps_bench_store:fetch_publish_events(),
    RecvEvents = ps_bench_store:fetch_recv_events(),

    % Now compare them: For each publish, who SHOULD have received it vs who DID receive it?
    Results = calculate_pbac_correctness(PublishEvents, RecvEvents),

    % Write the results to a CSV file so we can see if PBAC is working
    OutputFile = filename:join(OutDir, "pbac_correctness.csv"),
    write_results(OutputFile, Results),

    io:format("PBAC Correctness metrics written to ~s~n", [OutputFile]),
    ok.

%% Load purpose mappings from scenario configuration
%% This should be called by the test framework after loading the scenario
%% Input: PurposeMappingList from the scenario's purpose_mapping section
%% Example: [{factory_sensor, [{message_purpose, "production-metrics/{output,quality,.}"}]},
%%           {subscriber_01, [{subscription_purpose, "production-metrics/quality"},
%%                            {expected_data_sources, [factory_sensor]}]}]
load_purpose_mappings(PurposeMappingList) when is_list(PurposeMappingList) ->
    io:format("Loading purpose mappings for ~p clients...~n", [length(PurposeMappingList)]),
    ok = ps_bench_purpose_store:store_purpose_mapping(PurposeMappingList),
    io:format("Purpose mappings loaded successfully!~n"),
    ok;
load_purpose_mappings(_) ->
    io:format("WARNING: Invalid purpose mapping configuration~n"),
    ok.

%%%===================================================================
%%% Core logic - the meat of the plugin
%%%===================================================================

%% Main function: Calculate PBAC correctness
%% This is where we figure out if the broker is doing its job!
calculate_pbac_correctness(PublishEvents, RecvEvents) ->
    % First, reorganize received events to make it easy to look them up
    % Key = {PublisherID, Topic, SequenceNumber} → [List of who received it]
    RecvByPubTopic = group_recv_by_publisher_topic(RecvEvents),

    % For each message that was published, check:
    % - Who SHOULD have received it? (based on purpose compatibility)
    % - Who DID receive it? (from RecvByPubTopic)
    % - Did they match?
    Results = lists:foldl(fun(PubEvent, Acc) ->
        analyze_publish_event(PubEvent, RecvByPubTopic, Acc)
    end, #{correct => 0, erroneous_allow => 0, erroneous_reject => 0, total_expected => 0}, PublishEvents),

    % Calculate percentages so we can see accuracy at a glance
    Total = maps:get(correct, Results) + maps:get(erroneous_allow, Results) + maps:get(erroneous_reject, Results),

    case Total of
        0 ->
            % No data? Everything is 0%
            Results#{
                correct_pct => 0.0,
                erroneous_allow_pct => 0.0,
                erroneous_reject_pct => 0.0
            };
        _ ->
            % Calculate what percentage were correct/erroneous
            Results#{
                correct_pct => (maps:get(correct, Results) / Total) * 100,
                erroneous_allow_pct => (maps:get(erroneous_allow, Results) / Total) * 100,
                erroneous_reject_pct => (maps:get(erroneous_reject, Results) / Total) * 100
            }
    end.

%% Reorganize receive events for easy lookup
%% Input: List of "Client X received message Y"
%% Output: Map of {PublisherID, Topic, SequenceNum} → [List of receivers]
%% This makes it easy to ask "For message #5 on topic 'temp', who received it?"
group_recv_by_publisher_topic(RecvEvents) ->
    lists:foldl(fun(RecvEvent, Acc) ->
        % Parse the receive event
        % Format: {RecvNode, RecvClient, PublisherID, Topic, Seq, PubTimeNs, RecvTimeNs, Bytes}
        {_RecvNode, RecvClient, PublisherID, Topic, Seq, _PubTime, _RecvTime, _Bytes} = RecvEvent,

        % Build key = unique identifier for this message
        Key = {PublisherID, Topic, Seq},

        % Add this receiver to the list of who got this message
        Receivers = maps:get(Key, Acc, []),
        Acc#{Key => [RecvClient | Receivers]}
    end, #{}, RecvEvents).

%% Analyze a single publish event
%% For this message, figure out: Who got it vs who SHOULD have got it?
analyze_publish_event(PubEvent, RecvByPubTopic, Acc) ->
    % Parse the publish event
    % Format: {Node, ClientName, Topic, SeqId}
    {_Node, ClientName, Topic, SeqId} = PubEvent,

    % Look up who actually received this message
    Key = {ClientName, Topic, SeqId},
    ActualReceivers = maps:get(Key, RecvByPubTopic, []),

    % DEBUG: Only log first message to avoid spam
    case SeqId of
        1 -> io:format("~n[PBAC-DEBUG] Analyzing Publisher=~p Seq=~p ActualReceivers=~p~n", [ClientName, SeqId, ActualReceivers]);
        _ -> ok
    end,

    % Now the KEY question: Who SHOULD have received it based on purpose compatibility?
    % We need to compare:
    % - MP (Message Purpose): What purposes the publisher allowed
    % - SP (Subscription Purpose): What purpose each subscriber has
    % If SP is compatible with MP, they SHOULD receive it. Otherwise, they SHOULD NOT.

    % Try to get message metadata (MP and list of expected subscribers)
    case get_message_metadata(ClientName, Topic, SeqId) of
        {ok, MP, ExpectedSubscribers} ->
            % We know who should have received it!
            % Now compare expected vs actual

            % How many expected subscribers actually got it? (Correct deliveries)
            % How many expected subscribers DIDN'T get it? (Erroneous rejections)
            {Correct, ErrReject} = check_expected_subscribers(ExpectedSubscribers, ActualReceivers),

            % How many subscribers got it that SHOULDN'T have? (Erroneous allowances)
            % This is the security problem - unauthorized data access!
            ErrAllow = length(ActualReceivers) - Correct,

            % Update counters
            Acc#{
                correct => maps:get(correct, Acc) + Correct,
                erroneous_allow => maps:get(erroneous_allow, Acc) + max(0, ErrAllow),
                erroneous_reject => maps:get(erroneous_reject, Acc) + ErrReject,
                total_expected => maps:get(total_expected, Acc) + length(ExpectedSubscribers)
            };
        {error, _Reason} ->
            % Can't get metadata - maybe not configured yet?
            % Assume all deliveries are correct (we can't verify)
            Acc#{correct => maps:get(correct, Acc) + length(ActualReceivers)}
    end.

%% Check how many expected subscribers actually received the message
%% Returns: {Correct, ErrReject}
%% - Correct: # of expected subscribers who received it ✅
%% - ErrReject: # of expected subscribers who DIDN'T receive it ❌
check_expected_subscribers(ExpectedSubscribers, ActualReceivers) ->
    % Extract device types from actual receivers for fuzzy matching
    % e.g., "runner1_subscriber_1" -> "subscriber"
    ActualReceiverTypes = lists:map(fun(Receiver) ->
        extract_device_type_from_name(Receiver)
    end, ActualReceivers),

    % Also extract device types from expected subscribers
    % e.g., "subscriber_01" -> "subscriber"
    ExpectedTypes = lists:map(fun(Expected) ->
        extract_device_type_from_name(Expected)
    end, ExpectedSubscribers),

    % Count how many expected types are in the actual receiver types
    Correct = length([E || E <- ExpectedTypes, lists:member(E, ActualReceiverTypes)]),
    % The rest didn't get it even though they should have
    ErrReject = length(ExpectedSubscribers) - Correct,
    {Correct, ErrReject}.

%% Extract device type from a client name (for fuzzy matching)
%% "runner1_subscriber_1" -> "subscriber"
%% "subscriber_01" -> "subscriber"
extract_device_type_from_name(NameBin) when is_binary(NameBin) ->
    NameStr = binary_to_list(NameBin),
    Parts = string:split(NameStr, "_", all),
    case Parts of
        [_NodeOrFirst | RestParts] ->
            % Remove numeric suffix if present
            case lists:reverse(RestParts) of
                [LastPart | RestReversed] ->
                    case string:to_integer(LastPart) of
                        {_Index, ""} ->
                            % Last part is a number, remove it
                            DeviceType = string:join(lists:reverse(RestReversed), "_"),
                            list_to_binary(DeviceType);
                        _ ->
                            % Last part is not a number, keep all parts except first
                            list_to_binary(string:join(RestParts, "_"))
                    end;
                [] ->
                    % Only one part after first, return it
                    case RestParts of
                        [SinglePart] -> list_to_binary(SinglePart);
                        _ -> NameBin
                    end
            end;
        _ ->
            NameBin
    end;
extract_device_type_from_name(Name) when is_atom(Name) ->
    extract_device_type_from_name(atom_to_binary(Name, utf8));
extract_device_type_from_name(Name) ->
    Name.

%% Get message metadata: MP and list of expected subscribers
%% This is the CRITICAL function for PBAC checking!
%% Returns: {ok, MP, [List of compatible subscribers]} or {error, Reason}
get_message_metadata(ClientName, _Topic, _SeqId) ->
    ClientNameBin = ps_bench_utils:convert_to_binary(ClientName),

    % Step 1: Get the MP (Message Purpose) for this publisher
    % Use fallback to handle name mismatches (e.g., runner1_factory_sensor_1 -> factory_sensor)
    case ps_bench_purpose_store:get_message_purpose_with_fallback(ClientNameBin) of
        {ok, MP} ->
            % Step 2: Parse the MP to get all allowed purposes
            MPPurposes = parse_purpose_filter(MP),

            % Step 3: Get all subscribers and check which ones are compatible
            AllSubscribers = ps_bench_purpose_store:get_all_subscribers(),

            % Step 4: For each subscriber, check if their SP is compatible with this MP
            ExpectedSubscribers = lists:filtermap(fun(SubscriberName) ->
                % Use fallback for subscriber lookups too
                case ps_bench_purpose_store:get_subscription_purpose_with_fallback(SubscriberName) of
                    {ok, SP} ->
                        % Check if this subscription purpose is compatible
                        case is_purpose_compatible(MP, SP) of
                            true ->
                                % Also check if this publisher is in the expected_data_sources
                                case ps_bench_purpose_store:get_expected_sources_with_fallback(SubscriberName) of
                                    {ok, Sources} ->
                                        % Check if this publisher is an expected source
                                        % Need to check both exact name and device type since Sources might be stored as device types
                                        IsExpectedSource = lists:member(ClientNameBin, Sources) orelse
                                                          is_publisher_in_sources(ClientNameBin, Sources),
                                        case IsExpectedSource of
                                            true -> {true, SubscriberName};
                                            false -> false
                                        end;
                                    {error, not_found} ->
                                        % No expected sources specified, assume compatible based on purpose alone
                                        {true, SubscriberName}
                                end;
                            false ->
                                false
                        end;
                    {error, not_found} ->
                        false
                end
            end, AllSubscribers),

            % DEBUG: Log what we found
            io:format("~n[PBAC-DEBUG] Publisher=~p MP=~p~n", [ClientName, MP]),
            io:format("~n[PBAC-DEBUG] AllSubscribers=~p~n", [AllSubscribers]),
            io:format("~n[PBAC-DEBUG] ExpectedSubscribers=~p~n", [ExpectedSubscribers]),

            % Return the MP and list of expected subscribers
            {ok, MP, ExpectedSubscribers};
        {error, not_found} ->
            % No MP configured for this publisher
            {error, no_message_purpose_configured}
    end.

%% Check if a publisher matches any of the expected sources
%% Handles case where Sources contains device types (e.g., "factory_sensor")
%% and PublisherName is full name (e.g., "runner1_factory_sensor_1")
is_publisher_in_sources(PublisherNameBin, Sources) ->
    % Extract device type from publisher name
    % e.g., "runner1_factory_sensor_1" -> "factory_sensor"
    PublisherStr = binary_to_list(PublisherNameBin),
    Parts = string:split(PublisherStr, "_", all),
    DeviceType = case Parts of
        [_NodeName | DeviceParts] ->
            % Remove numeric suffix if present
            case lists:reverse(DeviceParts) of
                [LastPart | RestReversed] ->
                    case string:to_integer(LastPart) of
                        {_Index, ""} ->
                            % Last part is a number, remove it
                            list_to_binary(string:join(lists:reverse(RestReversed), "_"));
                        _ ->
                            % Last part is not a number, keep all device parts
                            list_to_binary(string:join(DeviceParts, "_"))
                    end;
                _ ->
                    PublisherNameBin
            end;
        _ ->
            PublisherNameBin
    end,
    % Check if device type is in the sources list
    lists:member(DeviceType, Sources).

%% Write results to a CSV file
%% This is what you'll look at to see if PBAC is working!
write_results(OutputFile, Results) ->
    % Make sure the output directory exists
    ok = filelib:ensure_dir(OutputFile),

    % Open the file for writing
    {ok, IoDevice} = file:open(OutputFile, [write]),

    % Write CSV header
    io:format(IoDevice, "metric,count,percentage~n", []),

    % Write the key metrics:

    % 1. Correct deliveries ✅
    %    Messages delivered to authorized subscribers
    io:format(IoDevice, "correct_deliveries,~p,~.2f~n",
              [maps:get(correct, Results), maps:get(correct_pct, Results, 0.0)]),

    % 2. Erroneous allowances ❌ (SECURITY PROBLEM!)
    %    Messages delivered to UNAUTHORIZED subscribers
    %    If this is > 0, the broker is NOT protecting data!
    io:format(IoDevice, "erroneous_allowances,~p,~.2f~n",
              [maps:get(erroneous_allow, Results), maps:get(erroneous_allow_pct, Results, 0.0)]),

    % 3. Erroneous rejections ❌ (RELIABILITY PROBLEM)
    %    Messages NOT delivered to authorized subscribers
    %    If this is > 0, authorized users aren't getting their data
    io:format(IoDevice, "erroneous_rejections,~p,~.2f~n",
              [maps:get(erroneous_reject, Results), maps:get(erroneous_reject_pct, Results, 0.0)]),

    % Total expected deliveries (baseline for comparison)
    io:format(IoDevice, "total_expected_deliveries,~p,100.00~n",
              [maps:get(total_expected, Results)]),

    % Overall accuracy: (correct / total_expected) * 100
    % Goal: 100% = perfect data protection!
    TotalExpected = maps:get(total_expected, Results),
    Accuracy = case TotalExpected of
        0 -> 0.0;
        _ -> (maps:get(correct, Results) / TotalExpected) * 100
    end,
    io:format(IoDevice, "accuracy,~p,~.2f~n", [maps:get(correct, Results), Accuracy]),

    file:close(IoDevice),
    ok.

%%%===================================================================
%%% Purpose compatibility checking helpers
%%% These implement the PBAC logic from the MQTT-DAP paper
%%%===================================================================

%% Check if a subscription purpose (SP) is compatible with a message purpose (MP)
%% Rule: SP must be a subset of MP
%% Example:
%%   MP = "production-metrics/{output,quality,.}"
%%   Expands to: ["production-metrics", "production-metrics/output", "production-metrics/quality"]
%%
%%   SP = "production-metrics/quality" → ✅ Compatible (it's in the MP list)
%%   SP = "inventory-tracking" → ❌ Incompatible (it's NOT in the MP list)
is_purpose_compatible(MP, SP) when is_binary(MP), is_binary(SP) ->
    % Parse both filters into lists of purposes
    MPPurposes = parse_purpose_filter(MP),
    SPPurposes = parse_purpose_filter(SP),

    % Check if EVERY purpose in SP is also in MP
    % If yes, compatible ✅. If no, incompatible ❌.
    lists:all(fun(SPurpose) ->
        lists:member(SPurpose, MPPurposes)
    end, SPPurposes).

%% Parse a purpose filter into a list of individual purposes
%% Handles brace expansion: {a,b,c}
%% Handles parent purpose: The dot (.) includes parent
%%
%% Examples:
%%   "production-metrics/{output,quality,.}" →
%%   ["production-metrics", "production-metrics/output", "production-metrics/quality"]
%%
%%   "billing/{electricity,gas}/{usage,cost}" →
%%   ["billing/electricity/usage", "billing/electricity/cost",
%%    "billing/gas/usage", "billing/gas/cost"]
parse_purpose_filter(Filter) when is_binary(Filter) ->
    FilterStr = binary_to_list(Filter),
    % Split by "/" to get parts
    Parts = string:split(FilterStr, "/", all),
    % Expand each part and combine
    expand_all_parts(Parts).

%% Expand all parts and generate all combinations
%% Input: ["billing", "{electricity,gas}", "{usage,cost}"]
%% Output: ["billing/electricity/usage", "billing/electricity/cost",
%%          "billing/gas/usage", "billing/gas/cost"]
expand_all_parts(Parts) ->
    expand_all_parts(Parts, [""]).

expand_all_parts([], Acc) ->
    % Remove leading "/" from each result and handle dot (.) for parent purposes
    Cleaned = lists:map(fun(Path) ->
        case Path of
            "/" ++ Rest -> Rest;
            _ -> Path
        end
    end, Acc),
    % Process dot notation to add parent purposes
    process_dot_notation(Cleaned);
expand_all_parts([Part | Rest], Acc) ->
    % Expand this part and combine with all existing paths
    Expansions = expand_single_part(Part),
    NewAcc = lists:flatmap(fun(Prefix) ->
        lists:map(fun(Expansion) ->
            Prefix ++ "/" ++ Expansion
        end, Expansions)
    end, Acc),
    expand_all_parts(Rest, NewAcc).

%% Expand a single part that may contain brace expansion
%% Examples:
%%   "{a,b,c}" → ["a", "b", "c"]
%%   "{output,quality,.}" → ["output", "quality", "."]
%%   "simple" → ["simple"]
expand_single_part(Part) ->
    case string:str(Part, "{") of
        0 ->
            % No braces, return as-is
            [Part];
        _ ->
            % Has braces, extract and split
            % Find content between { and }
            case extract_brace_content(Part) of
                {ok, Content} ->
                    % Split by comma
                    string:split(Content, ",", all);
                error ->
                    % Malformed, return as-is
                    [Part]
            end
    end.

%% Extract content from within braces
%% "{a,b,c}" → {ok, "a,b,c"}
extract_brace_content(Part) ->
    case string:str(Part, "{") of
        0 -> error;
        Start ->
            case string:str(Part, "}") of
                0 -> error;
                End ->
                    % Extract substring between braces
                    Content = string:sub_string(Part, Start + 1, End - 1),
                    {ok, Content}
            end
    end.

%% Process dot notation to add parent purposes
%% The dot (.) means "include the parent purpose"
%% Example: "production-metrics/{output,quality,.}" should include "production-metrics"
%% Input: ["production-metrics/output", "production-metrics/quality", "production-metrics/."]
%% Output: ["production-metrics/output", "production-metrics/quality", "production-metrics"]
process_dot_notation(Purposes) ->
    % Process each purpose
    Processed = lists:flatmap(fun(Purpose) ->
        case lists:suffix("/.", Purpose) of
            true ->
                % This purpose ends with "/." - replace it with the parent
                Parent = string:sub_string(Purpose, 1, length(Purpose) - 2),
                [Parent];
            false ->
                % No dot notation
                [Purpose]
        end
    end, Purposes),
    % Remove duplicates
    lists:usort(Processed).
