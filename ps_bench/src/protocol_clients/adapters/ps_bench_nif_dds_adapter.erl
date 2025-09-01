-module(ps_bench_nif_dds_adapter).
-behaviour(gen_server).

-include("ps_bench_config.hrl").

-define(SUBSCRIPTION_ETS_TABLE_NAME, current_dds_subscriptions).

% Interface currently needs to:
% - Accept handlers for recv, disconnect, connect
% - Not fail if the publication doesn't go through
% - Be noop if connect is called when already connected or disconnect is called when not connected
% - Return {ok, new_connection} or {ok, already_connected} for connect, reconnect, connect_clean

%% public
-export([start_link/6]).

%% gen_server callbacks
-export([init/1, handle_call/3,
         handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

start_link(TestName, NifName, FullNifPath, DomainId, ClientName, DeviceType) ->
    ClientNameAtom = ps_bench_utils:convert_to_atom(ClientName),
    gen_server:start_link({local, ClientNameAtom}, ?MODULE, [TestName, NifName, FullNifPath, ClientNameAtom, DeviceType, DomainId], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([TestName, NifName, FullNifPath, ClientName, DeviceType, DomainId]) ->

    % Initialize the NIF if not already done
    NifModule  = ps_bench_utils:convert_to_atom(NifName),
    ok = initialize_nif(NifModule, FullNifPath),
    {ok, ConfigFile} = ps_bench_config_manager:fetch_dds_config_file_path(),
    {ok, Participant} = retrieve_or_create_participant(NifModule, DomainId, ConfigFile),

    Tid = ensure_subs_table(),
    {ok, #{ test_name        => TestName
          , nif_module       => NifModule
          , client_name      => ClientName
          , device_type      => DeviceType
          , participant      => Participant
          , subscriber       => undefined
          , publisher        => undefined
          , pub_task         => 0
          , discon_task      => 0
          , recon_task       => 0
          , tid              => Tid
          }}.

%% helpers 
ensure_subs_table() ->
    case ets:whereis(?SUBSCRIPTION_ETS_TABLE_NAME) of
        undefined -> ets:new(?SUBSCRIPTION_ETS_TABLE_NAME, [set, public, named_table]);
        Tid       -> Tid
    end.

% For direct commands, we just forward these messages to the actual client
handle_call(connect, _From, State = #{nif_module := NifModule, participant := Participant, publisher := Publisher, client_name := ClientName}) ->
    do_connect(NifModule, ClientName, Participant, Publisher, State);

handle_call(connect_clean, _From, State = #{nif_module := NifModule, participant := Participant, publisher := Publisher, client_name := ClientName}) ->
    do_connect(NifModule, ClientName, Participant, Publisher, State);

handle_call(reconnect, _From, State = #{nif_module := NifModule, participant := Participant, publisher := Publisher, client_name := ClientName}) ->
    do_connect(NifModule, ClientName, Participant, Publisher, State);

handle_call(subscribe, _From, State = #{nif_module := NifModule, participant := Participant, subscriber := Subscriber, client_name := ClientName, tid := Tid}) ->
    case Subscriber of
        undefined ->
            % Create the subscriber for this client
            {ok, QoSProfile} = ps_bench_config_manager:fetch_dds_qos_profile(),
            {ok, NewSubscriber} = NifModule:create_subscriber_on_topic(?DDS_TOPIC, atom_to_list(ClientName), Participant, self(), QoSProfile),

            % Save the fact we were subscribed to this topic for disconnections
            PrevTopics =
            case ets:lookup(Tid, ClientName) of
                []              -> [];
                [{_, TopicList}]-> TopicList
            end,
            
            % Keep unique topics
            NewTopicList = lists:usort([?DDS_TOPIC] ++ PrevTopics),
            ets:insert(Tid, {ClientName, NewTopicList}),
            {reply, ok, State#{subscriber := NewSubscriber}};

        _ ->
            % We're fine, do nothing
            {reply, ok, State}
    end;

handle_call({publish, SeqId, Data}, _From, State = #{client_name := ClientName, nif_module := NifModule, publisher := Publisher}) -> 
    case NifModule:publish_message(SeqId, Data, Publisher) of
        ok ->
            ps_bench_store:record_publish(ClientName, ?DDS_TOPIC, SeqId);
        not_connected ->
            ok
    end,
    {reply, ok, State};

handle_call(unsubscribe, _From, State = #{nif_module := NifModule, participant := Participant, subscriber := Subscriber, client_name := ClientName, tid := Tid}) ->  
    case Subscriber of
        undefined ->
            % We're fine, do nothing
            {reply, ok, State};
        _ ->
            % Delete the subscriber
            ok = NifModule:delete_subscriber(Participant, Subscriber),

            % Update topic list to REMOVE the given topics
            case ets:lookup(Tid, ClientName) of
                [] -> ok;
                [{_, SubTopicList}] ->
                    Remaining =
                        lists:filter(
                        fun(Elem) ->
                            %% keep only those NOT matching this topic
                            Elem =:= ?DDS_TOPIC
                        end,
                        SubTopicList),
                    ets:insert(Tid, {ClientName, Remaining})
            end,
            {reply, ok, State#{subscriber := undefined}}
    end;

handle_call(disconnect, _From, State = #{nif_module := NifModule, participant := Participant, publisher := Publisher, subscriber := Subscriber, client_name := ClientName}) ->
    do_disconnect(NifModule, ClientName, Participant, Publisher, Subscriber),
    {reply, ok, State#{publisher := undefined, subscriber := undefined}};


handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

% Calls to start the actual timer functions
handle_cast(start_client_loops, State = #{device_type := DeviceType, client_name := ClientName}) ->

    % Subscriber clients don't have client loops
    case DeviceType of
        subscriber ->
            {noreply, State};
        _ ->
            {ok, PubTaskRef} = start_publication_loop(DeviceType, ClientName),
            {ok, DisconLoopTaskRef} = start_disconnection_loop(DeviceType, ClientName),
            {ok, ReconLoopTaskRef} = start_reconnection_loop(DeviceType, ClientName),
        {noreply, State#{pub_task := PubTaskRef, discon_task := DisconLoopTaskRef, recon_task := ReconLoopTaskRef}}
    end;

handle_cast(stop, State = #{pub_task := PubTaskRef, discon_task := DisconLoopTaskRef, recon_task := ReconLoopTaskRef}) ->
    % Stop all loops
    timer:cancel(PubTaskRef),
    timer:cancel(DisconLoopTaskRef),
    timer:cancel(ReconLoopTaskRef),
    {noreply, State}.

handle_info({?PUBLISH_RECV_MSG, {ClientName, Topic, SeqId, PubTimeNs, RecvTimeNs, Bytes, PublisherId}}, State) ->
    % Extracted needed info and store
    AtomId = list_to_atom(PublisherId),
    ps_bench_store:record_recv(ClientName, Topic, SeqId, PubTimeNs, RecvTimeNs, Bytes, AtomId),
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

start_publication_loop(DeviceType, ClientName) ->
    % Lookup needed parameters
    {ok, PubFrequencyMs} = ps_bench_config_manager:fetch_device_publication_frequency(DeviceType),
    {ok, PayloadSizeMean, PayloadSizeVariance} = ps_bench_config_manager:fetch_device_payload_info(DeviceType),

    % Create task
    timer:apply_interval(PubFrequencyMs, fun publication_loop/3, [ClientName, PayloadSizeMean, PayloadSizeVariance]).

publication_loop(ClientName, PayloadSizeMean, PayloadSizeVariance) ->
    {SeqId, Payload} = ps_bench_utils:generate_dds_datatype_data(PayloadSizeMean, PayloadSizeVariance),
    gen_server:call(ClientName, {publish, SeqId, Payload}).

start_disconnection_loop(DeviceType, ClientName) ->
    % Lookup needed parameters
    {ok, DisconPeriodMs, DisconChance} = ps_bench_config_manager:fetch_device_disconnect_info(DeviceType),

    % Make sure we have a non-zero period
    case DisconPeriodMs of 
        Period when Period > 0 ->
            % apply_repeatedly doesn't run a new instance until the previous finished
            timer:apply_repeatedly(Period, fun disconnect_loop/2, [ClientName, DisconChance]);
        _ ->
            {ok, 0}
    end.

disconnect_loop(ClientName, DisconChance) ->
    % Check to see if we should reconnect
    case ps_bench_utils:evaluate_uniform_chance(DisconChance) of
        true ->
            gen_server:call(ClientName, disconnect);
        false ->
            ok
    end.

start_reconnection_loop(DeviceType, ClientName) ->
    % Lookup needed parameters
    {ok, ReconPeriodMs, ReconChance} = ps_bench_config_manager:fetch_device_reconnect_info(DeviceType),

    % Make sure we have a non-zero period
    case ReconPeriodMs of 
        Period when Period > 0 ->
            % apply_repeatedly doesn't run a new instance until the previous finished
            timer:apply_repeatedly(Period, fun reconnect_loop/2, [ClientName, ReconChance]);
        _ ->
            {ok, 0}
    end.

reconnect_loop(ClientName, ReconnectChance) ->
    % Check to see if we should reconnect
    case ps_bench_utils:evaluate_uniform_chance(ReconnectChance) of
        true ->
            case gen_server:call(ClientName, reconnect) of
                {ok, new_connection} ->
                    % Now resubscribe to the needed topics
                    resub_to_previous_topics(ClientName);
                _ ->
                    ok
            end;
        false ->
            ok
    end.

resub_to_previous_topics(ClientName) ->
    case ets:whereis(?SUBSCRIPTION_ETS_TABLE_NAME) of 
        undefined ->
            ok;
        Tid ->
            case ets:take(Tid, ClientName) of
                [] ->
                    ok;
                [{ClientName, _Topics}] ->
                    % Create the subscriber for this client
                    gen_server:call(ClientName, subscribe);
                _ ->
                    ok
            end
    end.

terminate(Reason, _State = #{test_name := TestName, client_name := ClientName}) ->
    ps_bench_utils:log_message("DDS adapter terminating. reason=~p test=~p client=~p~n",
              [Reason, TestName, ClientName]),
    ok.

code_change(_OldVsn, State, _Extra) -> 
    {ok, State}.

initialize_nif(NifModule, FullNifPath) ->
    % Check if persistent term exists for the NIF having been loaded
    case persistent_term:get({?MODULE, initialized}, undefined) of
        true ->
            ok;
        _ ->
            NifModule:init(FullNifPath),
            persistent_term:put({?MODULE, initialized}, true)
    end.

retrieve_or_create_participant(NifModule, DomainId, ConfigFile) ->

    % Make sure the participant is created
    case persistent_term:get({?MODULE, participant, DomainId}, undefined) of
        % It's possible another thread is creating it, in which case we delay for it to be created
        creating ->
            receive
            after 500 ->
                retrieve_or_create_participant(NifModule, DomainId, ConfigFile)
            end;
        % Create if it doesn't exist
        undefined ->
            ok = persistent_term:put({?MODULE, participant, DomainId}, creating),

            {ok, QoSProfile} = ps_bench_config_manager:fetch_dds_qos_profile(),

            % Do creation and update with participant reference
            case NifModule:create_participant(DomainId, ConfigFile, QoSProfile) of 
                {ok, Participant} ->
                    ok = persistent_term:put({?MODULE, participant, DomainId}, Participant),
                    {ok, Participant};
                Error ->
                    ps_bench_utils:log_message("ERROR: Failed to create DDS participant with error ~p", [Error]),
                    Error
            end;
        % Retrieve if it already exists
        Participant ->
            {ok, Participant}
    end.

do_connect(NifModule, ClientName, Participant, Publisher, State) ->
    % Since we keep the participant active, "connection" here just means having an active publisher
    % capable of sending messages. We manage subscribers seperately under subscribe and unsubscribe calls
    case Publisher of
        undefined ->
            % Create the publisher for this client
            {ok, QoSProfile} = ps_bench_config_manager:fetch_dds_qos_profile(),
            {ok, NodeName} = ps_bench_config_manager:fetch_node_name(),
            {ok, NewPublisher} = NifModule:create_publisher_on_topic(?DDS_TOPIC, Participant, QoSProfile, atom_to_list(NodeName)),

            % We've connected at this point, save data
            TimeNs = erlang:system_time(nanosecond),
            ps_bench_store:record_connect(ClientName, TimeNs),
            {reply, {ok, new_connection}, State#{publisher := NewPublisher}};
        _ ->
            % We're fine, do nothing
            {reply, {ok, already_connected}, State}
    end.

do_disconnect(NifModule, ClientName, Participant, Publisher, Subscriber) ->
    % Need to delete both the publisher and subscriber
    case Publisher of
        undefined ->
            % We're fine, do nothing
            ok;
        _ ->
            % Delete the publisher
            NifModule:delete_publisher(Participant, Publisher)
    end,
    case Subscriber of
        undefined ->
            % We're fine, do nothing
            ok;
        _ ->
            % Delete the publisher
            NifModule:delete_subscriber(Participant, Subscriber)
    end,
    TimeNs = erlang:system_time(nanosecond),
    ps_bench_store:record_disconnect(ClientName, TimeNs, expected).