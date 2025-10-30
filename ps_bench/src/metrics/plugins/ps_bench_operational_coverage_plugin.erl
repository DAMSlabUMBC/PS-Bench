-module(ps_bench_operational_coverage_plugin).
-export([init/1, calc/0]).

-record(state, {
    output_dir :: string()  % Where to write results
}).

-define(STATE_KEY, {?MODULE, state}).

%%%===================================================================
%%% Plugin callbacks (PS-Bench calls these)
%%%===================================================================

%% Initialize the plugin
init(OutDir) ->
    State = #state{output_dir = OutDir},
    persistent_term:put(?STATE_KEY, State),
    io:format("Operational Coverage Plugin initialized. Output: ~s~n", [OutDir]),
    ok.

%% Calculate operational coverage metrics
%% This runs at the END of the test to see if operations reached all subscribers
calc() ->
    State = persistent_term:get(?STATE_KEY),
    OutDir = State#state.output_dir,

    io:format("Calculating operational coverage metrics...~n"),

    % Get all receive events (every time someone received a message)
    RecvEvents = ps_bench_store:fetch_recv_events(),

    % Build a map: Publisher → [List of subscribers who received their data]
    % Example: "factory_sensor_01" → ["subscriber_01", "subscriber_02", "subscriber_03"]
    PubSubMap = build_publisher_subscriber_map(RecvEvents),

    % Find all operational messages (delete, access, rectify, etc.)
    % These are messages on special topics like "dap/operation/request"
    OperationalEvents = fetch_operational_events(RecvEvents),

    % For each publisher's operations, calculate:
    % - How many data subscribers got the operation?
    % - What's the coverage percentage?
    Coverage = calculate_operational_coverage(PubSubMap, OperationalEvents),

    % Write results to CSV
    OutputFile = filename:join(OutDir, "operational_coverage.csv"),
    write_coverage_results(OutputFile, Coverage),

    io:format("Operational Coverage metrics written to ~s~n", [OutputFile]),
    ok.

%%%===================================================================
%%% Core logic
%%%===================================================================

%% Build a map of Publisher → Set of subscribers who received their data
%% This tells us: "For each publisher, who are all the subscribers that got data?"
%% We need this to calculate coverage: Did all data subscribers also get operations?
build_publisher_subscriber_map(RecvEvents) ->
    lists:foldl(fun(RecvEvent, Acc) ->
        % Parse the receive event
        case RecvEvent of
            {_RecvNode, RecvClient, _PubNode, _PubClient, _Topic, _Seq, _PubTime, _RecvTime, _Bytes, PublisherID} ->
                % Get the current set of subscribers for this publisher
                Subscribers = maps:get(PublisherID, Acc, sets:new()),
                % Add this subscriber to the set (sets automatically handle duplicates)
                NewSubscribers = sets:add_element(RecvClient, Subscribers),
                Acc#{PublisherID => NewSubscribers};
            _ ->
                % Old event format without PublisherID, skip it
                Acc
        end
    end, #{}, RecvEvents).

%% Extract only operational events from all receive events
%% Operational events are GDPR operations: delete, access, rectify, etc.
%% They use special topics like:
%% - "dap/operation/request" - Operation requests
%% - "dap/reg/SP/..." - Purpose registration
%% - "dap/response/..." - Operation responses
fetch_operational_events(RecvEvents) ->
    lists:filter(fun(RecvEvent) ->
        case RecvEvent of
            {_RecvNode, _RecvClient, _PubNode, _PubClient, Topic, _Seq, _PubTime, _RecvTime, _Bytes, _PublisherID} ->
                % Check if this topic is an operational topic
                is_operational_topic(Topic);
            _ ->
                false
        end
    end, RecvEvents).

%% Check if a topic is an operational topic
%% Operational topics contain: "dap/operation", "dap/reg", or "dap/response"
is_operational_topic(Topic) when is_binary(Topic) ->
    case binary:match(Topic, [<<"dap/operation">>, <<"dap/reg">>, <<"dap/response">>]) of
        nomatch -> false;  % Regular data topic
        _ -> true          % Operational topic
    end;
is_operational_topic(_) ->
    false.

%% Calculate operational coverage metrics
%% For each publisher, figure out:
%% - How many subscribers got data from them?
%% - How many of those subscribers got the operations?
%% - What's the coverage percentage?
calculate_operational_coverage(PubSubMap, OperationalEvents) ->
    % First, group operations by publisher
    % Key = PublisherID → [List of operations from that publisher]
    OpsByPub = group_operations_by_publisher(OperationalEvents),

    % For each publisher, calculate coverage
    maps:fold(fun(PublisherID, Operations, Acc) ->
        % Get all subscribers who received DATA from this publisher
        % Example: ["subscriber_01", "subscriber_02", "subscriber_03"]
        DataSubscribers = maps:get(PublisherID, PubSubMap, sets:new()),
        TotalDataSubs = sets:size(DataSubscribers),

        % Group this publisher's operations by type (delete, access, etc.)
        OpsByType = group_operations_by_type(Operations),

        % For each operation type, calculate coverage
        OpCoverage = maps:map(fun(OpType, OpEvents) ->
            % Get unique subscribers who received THIS operation
            OpSubscribers = extract_operation_subscribers(OpEvents),
            NumOpSubs = sets:size(OpSubscribers),

            % Calculate coverage: What % of data subscribers got the operation?
            % Example: 8 out of 10 data subscribers got delete = 80% coverage
            Coverage = case TotalDataSubs of
                0 -> 0.0;
                _ -> (NumOpSubs / TotalDataSubs) * 100
            end,

            % Count how many status responses we got (acknowledgments)
            StatusResponses = count_status_responses(OpEvents),

            #{
                total_data_subscribers => TotalDataSubs,
                operation_recipients => NumOpSubs,
                coverage_pct => Coverage,
                status_responses => StatusResponses,
                total_operations => length(OpEvents)
            }
        end, OpsByType),

        % Calculate OVERALL coverage for this publisher (across all operation types)
        % Union all subscribers who received ANY operation
        AllOpSubscribers = lists:foldl(fun(OpEvents, SubAcc) ->
            sets:union(SubAcc, extract_operation_subscribers(OpEvents))
        end, sets:new(), maps:values(OpsByType)),

        OverallCoverage = case TotalDataSubs of
            0 -> 0.0;
            _ -> (sets:size(AllOpSubscribers) / TotalDataSubs) * 100
        end,

        % Package up results for this publisher
        PublisherResults = #{
            total_data_subscribers => TotalDataSubs,
            operation_recipients => sets:size(AllOpSubscribers),
            overall_coverage_pct => OverallCoverage,
            by_operation_type => OpCoverage
        },

        Acc#{PublisherID => PublisherResults}
    end, #{}, OpsByPub).

%% Group operations by which publisher sent them
%% Input: List of all operational events
%% Output: Map of PublisherID → [List of their operations]
group_operations_by_publisher(OperationalEvents) ->
    lists:foldl(fun(OpEvent, Acc) ->
        case OpEvent of
            {_RecvNode, _RecvClient, _PubNode, _PubClient, _Topic, _Seq, _PubTime, _RecvTime, _Bytes, PublisherID} ->
                Ops = maps:get(PublisherID, Acc, []),
                Acc#{PublisherID => [OpEvent | Ops]};
            _ ->
                Acc
        end
    end, #{}, OperationalEvents).

%% Group operations by type (delete, access, rectify, etc.)
%% Input: List of operations from one publisher
%% Output: Map of OperationType → [List of operations of that type]
group_operations_by_type(Operations) ->
    lists:foldl(fun(OpEvent, Acc) ->
        % Figure out what type of operation this is
        OpType = extract_operation_type(OpEvent),
        Ops = maps:get(OpType, Acc, []),
        Acc#{OpType => [OpEvent | Ops]}
    end, #{}, Operations).

%% Extract the operation type from an operational event
%% Parses the topic to figure out: delete? access? rectify?
%% Example: "dap/operation/request/FetchContact" → "FetchContact"
extract_operation_type(OpEvent) ->
    case OpEvent of
        {_RecvNode, _RecvClient, _PubNode, _PubClient, Topic, _Seq, _PubTime, _RecvTime, _Bytes, _PublisherID} ->
            % Split topic by "/" and take the last part
            % This should be the operation name
            case binary:split(Topic, <<"/">>, [global]) of
                Parts when length(Parts) >= 3 ->
                    lists:last(Parts);
                _ ->
                    <<"unknown">>
            end;
        _ ->
            <<"unknown">>
    end.

%% Extract unique subscribers who received these operations
%% Input: List of operation events
%% Output: Set of subscriber IDs who received at least one
extract_operation_subscribers(OpEvents) ->
    lists:foldl(fun(OpEvent, Acc) ->
        case OpEvent of
            {_RecvNode, RecvClient, _PubNode, _PubClient, _Topic, _Seq, _PubTime, _RecvTime, _Bytes, _PublisherID} ->
                sets:add_element(RecvClient, Acc);
            _ ->
                Acc
        end
    end, sets:new(), OpEvents).

%% Count how many status responses (acknowledgments) we got
%% Status responses have "response" in the topic
count_status_responses(OpEvents) ->
    lists:foldl(fun(OpEvent, Count) ->
        case is_status_response(OpEvent) of
            true -> Count + 1;
            false -> Count
        end
    end, 0, OpEvents).

%% Check if this event is a status response
%% Responses have "response" in the topic (e.g., "dap/response/...")
is_status_response(OpEvent) ->
    case OpEvent of
        {_RecvNode, _RecvClient, _PubNode, _PubClient, Topic, _Seq, _PubTime, _RecvTime, _Bytes, _PublisherID} ->
            case binary:match(Topic, <<"response">>) of
                nomatch -> false;  % Not a response
                _ -> true          % This is a response
            end;
        _ ->
            false
    end.

%% Write coverage results to CSV file
%% This creates a readable report showing coverage for each publisher and operation
write_coverage_results(OutputFile, Coverage) ->
    % Make sure output directory exists
    ok = filelib:ensure_dir(OutputFile),
    {ok, IoDevice} = file:open(OutputFile, [write]),

    % Write CSV header
    io:format(IoDevice, "publisher_id,operation_type,total_data_subscribers,operation_recipients,coverage_pct,status_responses,total_operations~n", []),

    % Write results for each publisher
    maps:foreach(fun(PublisherID, PubResults) ->
        TotalDataSubs = maps:get(total_data_subscribers, PubResults),
        OverallRecipients = maps:get(operation_recipients, PubResults),
        OverallCoverage = maps:get(overall_coverage_pct, PubResults),

        % Write overall row for this publisher (all operations combined)
        io:format(IoDevice, "~s,OVERALL,~p,~p,~.2f,0,0~n",
                  [PublisherID, TotalDataSubs, OverallRecipients, OverallCoverage]),

        % Write a row for each operation type (delete, access, etc.)
        OpTypes = maps:get(by_operation_type, PubResults),
        maps:foreach(fun(OpType, OpResults) ->
            io:format(IoDevice, "~s,~s,~p,~p,~.2f,~p,~p~n",
                      [PublisherID,
                       OpType,
                       maps:get(total_data_subscribers, OpResults),
                       maps:get(operation_recipients, OpResults),
                       maps:get(coverage_pct, OpResults),
                       maps:get(status_responses, OpResults),
                       maps:get(total_operations, OpResults)])
        end, OpTypes)
    end, Coverage),

    % Write summary statistics at the bottom
    io:format(IoDevice, "~n# Summary Statistics~n", []),
    {TotalPubs, AvgCoverage} = calculate_summary_stats(Coverage),
    io:format(IoDevice, "# Total Publishers,~p~n", [TotalPubs]),
    io:format(IoDevice, "# Average Coverage,~.2f%~n", [AvgCoverage]),

    file:close(IoDevice),
    ok.

%% Calculate summary statistics across all publishers
%% Returns: {Total number of publishers, Average coverage percentage}
calculate_summary_stats(Coverage) ->
    TotalPubs = maps:size(Coverage),
    case TotalPubs of
        0 ->
            {0, 0.0};
        _ ->
            % Sum up all coverage percentages
            TotalCoverage = maps:fold(fun(_PubID, PubResults, Acc) ->
                Acc + maps:get(overall_coverage_pct, PubResults)
            end, 0.0, Coverage),
            % Calculate average
            AvgCoverage = TotalCoverage / TotalPubs,
            {TotalPubs, AvgCoverage}
    end.
