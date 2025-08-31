-module(ps_bench_default_dds_interface).

-export([init/1, create_participant/3, create_subscriber_on_topic/5, create_publisher_on_topic/4, publish_message/3, delete_subscriber/2, delete_publisher/2]).

-nifs([create_participant/3, create_subscriber_on_topic/5, create_publisher_on_topic/4, publish_message/3, delete_subscriber/2, delete_publisher/2]).

init(NifPath) ->
      ok = erlang:load_nif(NifPath, 0).

create_participant(_DomainId, _ConfigPath, _QoSProfile) ->
      erlang:nif_error("NIF library not loaded").

create_subscriber_on_topic(_TopicName, _ClientName, _Participant,  _ListenerPid, _QoSProfile) ->
      erlang:nif_error("NIF library not loaded").

create_publisher_on_topic(_TopicName, _Participant, _QosProfile, _PublishingNodeName) ->
      erlang:nif_error("NIF library not loaded").

publish_message(_Message, _Id, _Writer) ->
      erlang:nif_error("NIF library not loaded").

delete_subscriber(_Participant, _Subscriber) ->
      erlang:nif_error("NIF library not loaded").

delete_publisher(_Participant, _Publisher) ->
      erlang:nif_error("NIF library not loaded").