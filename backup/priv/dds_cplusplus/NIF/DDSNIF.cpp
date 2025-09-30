
#include <dds/DdsDcpsInfrastructureC.h>
#include <dds/DdsDcpsPublicationC.h>
#include <dds/DCPS/Marked_Default_Qos.h>
#include <dds/DCPS/Service_Participant.h>
#include <dds/DCPS/QOS_XML_Handler/QOS_XML_Loader.h>
#include <ace/Arg_Shifter.h>

#include <erl_nif.h>
#include <PsBenchTypeSupportImpl.h>
#include <PsBenchC.h>
#include "DataReaderListenerImpl.h"

#include <string>

// Define macro to avoid compiler warnings for unused API params
#define UNUSED_PARAM(expr) do { (void)(expr); } while (0)

ErlNifResourceType* PARTICIPANT_RES_TYPE;
ErlNifResourceType* PUBLISHER_RES_TYPE;
ErlNifResourceType* SUBSCRIBER_RES_TYPE;
ERL_NIF_TERM atom_ok;
ERL_NIF_TERM atom_not_connected;

typedef struct 
{
    DDS::DomainParticipant_ptr participant;
    OpenDDS::DCPS::QOS_XML_Loader* loader;
} ParticipantStruct;

typedef struct 
{
    DDS::Publisher_ptr publisher;
    PsBench::DeviceMessageDataWriter_ptr writer;
    char* nodeId;
} PublisherStruct;

typedef struct 
{
    DDS::Subscriber_ptr subscriber;
} SubscriberStruct;


void participant_free_res(ErlNifEnv* env, void* obj)
{
    UNUSED_PARAM(env);

    ParticipantStruct* participantStruct = (ParticipantStruct*) obj;
    participantStruct->participant->delete_contained_entities();
    DDS::DomainParticipantFactory_var dpf = TheParticipantFactory;
    dpf->delete_participant(participantStruct->participant);
}

void publisher_free_res(ErlNifEnv* env, void* obj)
{
    UNUSED_PARAM(env);
    UNUSED_PARAM(obj);
    
    PublisherStruct* publisherStruct = (PublisherStruct*) obj;
    if(publisherStruct->nodeId != 0)
    {
        free(publisherStruct->nodeId);
        publisherStruct->nodeId = 0;
    }
}

void subscriber_free_res(ErlNifEnv* env, void* obj)
{
    UNUSED_PARAM(env);
    UNUSED_PARAM(obj);
    // Do nothing, will be cleaned with participant
}

static int open_resource(ErlNifEnv* env)
{
    const char* mod = NULL;
    const char* name = "ParticipantStruct";
    ErlNifResourceFlags flags = (ErlNifResourceFlags)(ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER);

    PARTICIPANT_RES_TYPE = enif_open_resource_type(env, mod, name, participant_free_res, flags, NULL);
    if(PARTICIPANT_RES_TYPE == NULL) return -1;

    name = "PublisherStruct";
    PUBLISHER_RES_TYPE = enif_open_resource_type(env, mod, name, publisher_free_res, flags, NULL);
    if(PUBLISHER_RES_TYPE == NULL) return -1;

    name = "SubscriberStruct";
    SUBSCRIBER_RES_TYPE = enif_open_resource_type(env, mod, name, subscriber_free_res, flags, NULL);
    if(SUBSCRIBER_RES_TYPE == NULL) return -1;

    return 0;
}

static int load(ErlNifEnv* env, void** priv, ERL_NIF_TERM load_info)
{
    UNUSED_PARAM(priv);
    UNUSED_PARAM(load_info);

    if(open_resource(env) == -1) return -1;

    atom_ok = enif_make_atom(env, "ok");
    atom_not_connected = enif_make_atom(env, "not_connected");

    return 0;
}

// Erlang requires that we re-open resources on re-initialisation.
static int reload(ErlNifEnv* env, void** priv, ERL_NIF_TERM load_info)
{
    UNUSED_PARAM(priv);
    UNUSED_PARAM(load_info);

    if(open_resource(env) == -1) return -1;
    return 0;
}

static int reload(ErlNifEnv* env, void** priv, void** old_priv, ERL_NIF_TERM load_info)
{
    UNUSED_PARAM(priv);
    UNUSED_PARAM(old_priv);
    UNUSED_PARAM(load_info);

    if(open_resource(env) == -1) return -1;
    return 0;
}

static int upgrade(ErlNifEnv* env, void** priv, void** old_priv, ERL_NIF_TERM load_info)
{
    UNUSED_PARAM(env);
    UNUSED_PARAM(priv);
    UNUSED_PARAM(old_priv);
    UNUSED_PARAM(load_info);

    return 0;
}

static void unload(ErlNifEnv* caller_env, void* priv_data)
{
    UNUSED_PARAM(caller_env);
    UNUSED_PARAM(priv_data);

    TheServiceParticipant->shutdown();
}

ERL_NIF_TERM create_participant(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    UNUSED_PARAM(argc);

    ParticipantStruct* res;
    ERL_NIF_TERM ret;

    int domainId = 0;
    if(!enif_get_int(env, argv[0], &domainId))
    {
        return enif_make_badarg(env);
    }

    // Get config file path
    unsigned int configPathLength = 0;
    if(!enif_get_string_length(env, argv[1], &configPathLength, ERL_NIF_UTF8))
    {
        return enif_make_atom(env, "invalid_config_path");
    }

    char* configPath = (char*)malloc(configPathLength + 1);
    if(!enif_get_string(env, argv[1], configPath, configPathLength + 1, ERL_NIF_UTF8))
    {
        free(configPath);
        return enif_make_atom(env, "invalid_config_path");
    }

    // Get QoS profile
    unsigned int qosParamLength = 0;
    if(!enif_get_string_length(env, argv[2], &qosParamLength, ERL_NIF_UTF8))
    {
        return enif_make_atom(env, "invalid_qos_profile");
    }

    bool useQos = false;
    char* qosParam = nullptr;
    if(qosParamLength > 0)
    {
        useQos = true;
        qosParam = (char*)malloc(qosParamLength + 1);
        if(!enif_get_string(env, argv[2], qosParam, qosParamLength + 1, ERL_NIF_UTF8))
        {
            free(qosParam);
            return enif_make_atom(env, "invalid_qos_profile");
        }
    }

    TheServiceParticipant->default_configuration_file(ACE_TEXT(configPath));
    DDS::DomainParticipantFactory_var dpf = TheParticipantFactory;

    OpenDDS::DCPS::QOS_XML_Loader* xml_loader = nullptr;
    DDS::DomainParticipantQos partQos = PARTICIPANT_QOS_DEFAULT;

    if(useQos)
    {
        xml_loader = new OpenDDS::DCPS::QOS_XML_Loader();
        DDS::ReturnCode_t retcode = xml_loader->init(ACE_TEXT(qosParam));
        if(retcode != DDS::RETCODE_OK)
        {
            free(qosParam);
            return enif_make_atom(env, "malformed_qos");
        }

        // create domain participant
        retcode = xml_loader->get_participant_qos(partQos, ACE_TEXT(qosParam));
        if(retcode != DDS::RETCODE_OK)
        {
            free(qosParam);
            return enif_make_atom(env, "participant_profile_not_found");
        }
    }

    // create domain participant
    DDS::DomainParticipant_var participant =
        dpf->create_participant(domainId,
        partQos,
        0,
        OpenDDS::DCPS::DEFAULT_STATUS_MASK);

    if (0 == participant) 
        return enif_make_atom(env, "participant_failed");

    // Register PsBench type
    PsBench::DeviceMessageTypeSupport_var ts =
        new PsBench::DeviceMessageTypeSupportImpl();

    if (ts->register_type(participant.in(), "") != DDS::RETCODE_OK) 
        return enif_make_atom(env, "type_registration_failed");

    res = (ParticipantStruct*) enif_alloc_resource(PARTICIPANT_RES_TYPE, sizeof(ParticipantStruct));
    if(res == NULL) return enif_make_badarg(env);

    ret = enif_make_resource(env, res);
    enif_release_resource(res);

    res->participant = participant.in();
    res->loader = xml_loader;

    return enif_make_tuple2(env, atom_ok, ret);
}

ERL_NIF_TERM create_subscriber_on_topic(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    UNUSED_PARAM(argc);

    // Get topic
    unsigned int topicLength = 0;
    if(!enif_get_string_length(env, argv[0], &topicLength, ERL_NIF_UTF8))
    {
        return enif_make_atom(env, "invalid_topic");
    }

    char* topicName = (char*)malloc(topicLength + 1);
    if(!enif_get_string(env, argv[0], topicName, topicLength + 1, ERL_NIF_UTF8))
    {
        free(topicName);
        return enif_make_atom(env, "invalid_topic");
    }

    // Get client name
    unsigned int clientNameLength = 0;
    if(!enif_get_string_length(env, argv[1], &clientNameLength, ERL_NIF_UTF8))
    {
        return enif_make_atom(env, "invalid_client_name");
    }

    char* clientName = (char*)malloc(clientNameLength + 1);
    if(!enif_get_string(env, argv[1], clientName, clientNameLength + 1, ERL_NIF_UTF8))
    {
        free(clientName);
        return enif_make_atom(env, "invalid_client_name");
    }

    // Get participant
    void* voidStruct = nullptr;
    if(!enif_get_resource(env, argv[2], PARTICIPANT_RES_TYPE, &voidStruct))
    {
        return enif_make_atom(env, "failed_to_fetch_participant");
    }

    ParticipantStruct* participantStruct = (ParticipantStruct*)voidStruct;

    // Get return PID
    ErlNifPid* listenerPID = (ErlNifPid*)malloc(sizeof(ErlNifPid*));
    if(!enif_get_local_pid(env, argv[3], listenerPID))
    {
        return enif_make_atom(env, "invalid_listener_pid");
    }

    // Get QoS profile
    unsigned int qosParamLength = 0;
    if(!enif_get_string_length(env, argv[4], &qosParamLength, ERL_NIF_UTF8))
    {
        return enif_make_atom(env, "invalid_qos_profile");
    }

    bool useQos = false;
    char* qosParam = nullptr;
    if(qosParamLength > 0)
    {
        useQos = true;
        qosParam = (char*)malloc(qosParamLength + 1);
        if(!enif_get_string(env, argv[4], qosParam, qosParamLength + 1, ERL_NIF_UTF8))
        {
            free(qosParam);
            return enif_make_atom(env, "invalid_qos_profile");
        }
    }

    OpenDDS::DCPS::QOS_XML_Loader* xml_loader = participantStruct->loader;
    DDS::TopicQos topicQos = TOPIC_QOS_DEFAULT;
    DDS::SubscriberQos subscriberQos = SUBSCRIBER_QOS_DEFAULT;
    DDS::DataReaderQos datareaderQos = DATAREADER_QOS_DEFAULT;

    if(useQos && xml_loader != nullptr)
    {
        // Get topic QoS
        DDS::ReturnCode_t retcode = xml_loader->get_topic_qos(topicQos, ACE_TEXT(qosParam), ACE_TEXT(topicName));
        if(retcode != DDS::RETCODE_OK)
        {
            free(qosParam);
            return enif_make_atom(env, "topic_profile_not_found");
        }

        retcode = xml_loader->get_subscriber_qos(subscriberQos, ACE_TEXT(qosParam));
        if(retcode != DDS::RETCODE_OK)
        {
            free(qosParam);
            return enif_make_atom(env, "subscriber_profile_not_found");
        }

        retcode = xml_loader->get_datareader_qos(datareaderQos, ACE_TEXT(qosParam), ACE_TEXT(topicName));
        if(retcode != DDS::RETCODE_OK)
        {
            free(qosParam);
            return enif_make_atom(env, "datareader_profile_not_found");
        }
    }

    // create topic
    PsBench::DeviceMessageTypeSupport_var ts =
            new PsBench::DeviceMessageTypeSupportImpl();
    CORBA::String_var type_name = ts->get_type_name();
    DDS::Topic_var topic =
        participantStruct->participant->create_topic(topicName,
        type_name.in(),
        topicQos,
        0,
        OpenDDS::DCPS::DEFAULT_STATUS_MASK);

    if (0 == topic) 
        return enif_make_atom(env, "topic_creation_failed");

    // create subscriber
    DDS::Subscriber_var subscriber =
        participantStruct->participant->create_subscriber(subscriberQos,
        0,
        OpenDDS::DCPS::DEFAULT_STATUS_MASK);

    if (0 == subscriber) 
        return enif_make_atom(env, "subscriber_creation_failed");

    // create and narrow datareader, assigning listener
    DDS::DataReaderListener_var listener(new DataReaderListenerImpl(listenerPID, clientName, topicName));

    DDS::DataReader_var reader =
        subscriber->create_datareader(topic.in(),
        datareaderQos,
        listener.in(),
        OpenDDS::DCPS::DEFAULT_STATUS_MASK);

    if (0 == reader) 
        return enif_make_atom(env, "datareader_creation_failed");

    SubscriberStruct* res;
    ERL_NIF_TERM ret;

    res = (SubscriberStruct*) enif_alloc_resource(SUBSCRIBER_RES_TYPE, sizeof(SubscriberStruct));
    if(res == NULL) return enif_make_badarg(env);

    ret = enif_make_resource(env, res);
    enif_release_resource(res);

    res->subscriber = subscriber.in();

    return enif_make_tuple2(env, atom_ok, ret);
}

ERL_NIF_TERM delete_subscriber(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    UNUSED_PARAM(argc);

    // Get participant
    void* voidStruct = nullptr;
    if(!enif_get_resource(env, argv[0], PARTICIPANT_RES_TYPE, &voidStruct))
    {
        return enif_make_atom(env, "failed_to_fetch_participant");
    }

    ParticipantStruct* participantStruct = (ParticipantStruct*)voidStruct;

    // Get subscriber
    voidStruct = nullptr;
    if(!enif_get_resource(env, argv[1], SUBSCRIBER_RES_TYPE, &voidStruct))
    {
        return enif_make_atom(env, "failed_to_fetch_subscriber");
    }

    SubscriberStruct* subscriberStruct = (SubscriberStruct*)voidStruct;

    // Remove subscriber from participant
    if(!participantStruct->participant->delete_subscriber(subscriberStruct->subscriber) == DDS::RETCODE_OK)
    {
        return enif_make_atom(env, "failed_to_delete_subscriber");
    }

    return atom_ok;
}

ERL_NIF_TERM create_publisher_on_topic(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    UNUSED_PARAM(argc);

    PublisherStruct* res;
    ERL_NIF_TERM ret;

    // Get topic
    unsigned int topicLength = 0;
    if(!enif_get_string_length(env, argv[0], &topicLength, ERL_NIF_UTF8))
    {
        return enif_make_atom(env, "invalid_topic");
    }

    char* topicName = (char*)malloc(topicLength + 1);
    if(!enif_get_string(env, argv[0], topicName, topicLength + 1, ERL_NIF_UTF8))
    {
        free(topicName);
        return enif_make_atom(env, "invalid_topic");
    }

    // Get participant
    void* voidStruct = nullptr;
    if(!enif_get_resource(env, argv[1], PARTICIPANT_RES_TYPE, &voidStruct))
    {
        return enif_make_atom(env, "failed_to_fetch_participant");
    }

    ParticipantStruct* participantStruct = (ParticipantStruct*)voidStruct;

    // Get QoS profile
    unsigned int qosParamLength = 0;
    if(!enif_get_string_length(env, argv[2], &qosParamLength, ERL_NIF_UTF8))
    {
        return enif_make_atom(env, "invalid_qos_profile");
    }

    // Get publishing node ID
    unsigned int publishingNodeLength = 0;
    if(!enif_get_string_length(env, argv[3], &publishingNodeLength, ERL_NIF_UTF8))
    {
        return enif_make_atom(env, "invalid_node_id");
    }

    char* publishingNode = (char*)malloc(publishingNodeLength + 1);
    if(!enif_get_string(env, argv[3], publishingNode, publishingNodeLength + 1, ERL_NIF_UTF8))
    {
        free(publishingNode);
        return enif_make_atom(env, "invalid_node_id");
    }

    bool useQos = false;
    char* qosParam = nullptr;
    if(qosParamLength > 0)
    {
        useQos = true;
        qosParam = (char*)malloc(qosParamLength + 1);
        if(!enif_get_string(env, argv[2], qosParam, qosParamLength + 1, ERL_NIF_UTF8))
        {
            free(qosParam);
            return enif_make_atom(env, "invalid_qos_profile");
        }
    }

    OpenDDS::DCPS::QOS_XML_Loader* xml_loader = participantStruct->loader;
    DDS::TopicQos topicQos = TOPIC_QOS_DEFAULT;
    DDS::PublisherQos publisherQos = PUBLISHER_QOS_DEFAULT;
    DDS::DataWriterQos datawriterQos = DATAWRITER_QOS_DEFAULT;

    if(useQos && xml_loader != nullptr)
    {
        // Get topic QoS
        DDS::ReturnCode_t retcode = xml_loader->get_topic_qos(topicQos, ACE_TEXT(qosParam), ACE_TEXT(topicName));
        if(retcode != DDS::RETCODE_OK)
        {
            free(qosParam);
            return enif_make_atom(env, "topic_profile_not_found");
        }

        retcode = xml_loader->get_publisher_qos(publisherQos, ACE_TEXT(qosParam));
        if(retcode != DDS::RETCODE_OK)
        {
            free(qosParam);
            return enif_make_atom(env, "publisher_profile_not_found");
        }

        retcode = xml_loader->get_datawriter_qos(datawriterQos, ACE_TEXT(qosParam), ACE_TEXT(topicName));
        if(retcode != DDS::RETCODE_OK)
        {
            free(qosParam);
            return enif_make_atom(env, "datawriter_profile_not_found");
        }
    }

    // create topic
    PsBench::DeviceMessageTypeSupport_var ts =
            new PsBench::DeviceMessageTypeSupportImpl();
    CORBA::String_var type_name = ts->get_type_name();
    DDS::Topic_var topic =
        participantStruct->participant->create_topic(topicName,
        type_name.in(),
        topicQos,
        0,
        OpenDDS::DCPS::DEFAULT_STATUS_MASK);

    if (0 == topic) 
        return enif_make_atom(env, "topic_creation_failed");

    // create publisher
    DDS::Publisher_var publisher =
        participantStruct->participant->create_publisher(publisherQos,
        0,
        OpenDDS::DCPS::DEFAULT_STATUS_MASK);

    if (0 == publisher) 
        return enif_make_atom(env, "publisher_creation_failed");

    // create and narrow datawriter
    DDS::DataWriter_var writer =
        publisher->create_datawriter(topic.in(),
        datawriterQos,
        DDS::DataWriterListener::_nil(),
        OpenDDS::DCPS::DEFAULT_STATUS_MASK);

    if (0 == writer) 
        return enif_make_atom(env, "datawriter_creation_failed");

    PsBench::DeviceMessageDataWriter_var message_writer =
        PsBench::DeviceMessageDataWriter::_narrow(writer.in());

    if (0 == message_writer) 
        return enif_make_atom(env, "datawriter_narrow_failed");

    res = (PublisherStruct*) enif_alloc_resource(PUBLISHER_RES_TYPE, sizeof(PublisherStruct));
    if(res == NULL) return enif_make_badarg(env);

    ret = enif_make_resource(env, res);
    enif_release_resource(res);

    res->publisher = publisher.in();
    res->writer = message_writer.in();
    res->nodeId = publishingNode;

    return enif_make_tuple2(env, atom_ok, ret);
}

ERL_NIF_TERM delete_publisher(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    UNUSED_PARAM(argc);

    // Get participant
    void* voidStruct = nullptr;
    if(!enif_get_resource(env, argv[0], PARTICIPANT_RES_TYPE, &voidStruct))
    {
        return enif_make_atom(env, "failed_to_fetch_participant");
    }

    ParticipantStruct* participantStruct = (ParticipantStruct*)voidStruct;

    // Get subscriber
    voidStruct = nullptr;
    if(!enif_get_resource(env, argv[1], PUBLISHER_RES_TYPE, &voidStruct))
    {
        return enif_make_atom(env, "failed_to_fetch_publisher");
    }

    PublisherStruct* publisherStruct = (PublisherStruct*)voidStruct;

    // Remove subscriber from participant
    if(!participantStruct->participant->delete_publisher(publisherStruct->publisher) == DDS::RETCODE_OK)
    {
        return enif_make_atom(env, "failed_to_delete_publisher");
    }

    publisherStruct->publisher = 0;
    publisherStruct->writer = 0;
    free(publisherStruct->nodeId);
    publisherStruct->nodeId = 0;

    return atom_ok;
}

ERL_NIF_TERM publish_message(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    UNUSED_PARAM(argc);

    // Get message sequence Id
    int seqId = 0;
    if(!enif_get_int(env, argv[0], &seqId))
    {
        return enif_make_badarg(env);
    }

    // Get message contents
    ErlNifBinary messageData;
    if(!enif_inspect_binary(env, argv[1], &messageData))
    {
        return enif_make_atom(env, "invalid_message");
    }

    // Get datawriter
    void* voidStruct = nullptr;
    if(!enif_get_resource(env, argv[2], PUBLISHER_RES_TYPE, &voidStruct))
    {
        return atom_not_connected;
    }

    PublisherStruct* publisherStruct = (PublisherStruct*)voidStruct;

    if(publisherStruct->publisher != 0)
    {
        PsBench::DeviceMessage message;
        message.seq_id = seqId;
        message.payload.length(messageData.size);
        for(size_t i = 0; i < messageData.size; i++)
        {
            message.payload[i] = messageData.data[i];
        }
        message.publisher_id = CORBA::string_dup(publisherStruct->nodeId);
        
        DDS::ReturnCode_t err = publisherStruct->writer->write(message, DDS::HANDLE_NIL);
        if (err!=DDS::RETCODE_OK)
            return enif_make_atom(env, "write_failed");

        return atom_ok;
    }
    else
    {
        return atom_not_connected;
    }
}

static ErlNifFunc nif_funcs[] = {
    {"create_participant", 3, create_participant},
    {"create_subscriber_on_topic", 5, create_subscriber_on_topic},
    {"create_publisher_on_topic", 4, create_publisher_on_topic},
    {"publish_message", 3, publish_message},
    {"delete_subscriber", 2, delete_subscriber},
    {"delete_publisher", 2, delete_publisher}
};

ERL_NIF_INIT(ps_bench_default_dds_interface, nif_funcs, &load, &reload, &upgrade, &unload);