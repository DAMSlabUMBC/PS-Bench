#include <ace/OS_NS_stdlib.h>
#include <dds/DCPS/TimeSource.h>
#include "DataReaderListenerImpl.h"
#include <memory>

#include <iostream>

void
    DataReaderListenerImpl::on_requested_deadline_missed(
    DDS::DataReader_ptr /*reader*/,
    const DDS::RequestedDeadlineMissedStatus& /*status*/)
{
}

void
    DataReaderListenerImpl::on_requested_incompatible_qos(
    DDS::DataReader_ptr /*reader*/,
    const DDS::RequestedIncompatibleQosStatus& /*status*/)
{
}

void
    DataReaderListenerImpl::on_sample_rejected(
    DDS::DataReader_ptr /*reader*/,
    const DDS::SampleRejectedStatus& /*status*/)
{
}

void
    DataReaderListenerImpl::on_liveliness_changed(
    DDS::DataReader_ptr /*reader*/,
    const DDS::LivelinessChangedStatus& /*status*/)
{
}

void
    DataReaderListenerImpl::on_data_available(DDS::DataReader_ptr reader)
{
    PsBench::DeviceMessageDataReader_var reader_i =
        PsBench::DeviceMessageDataReader::_narrow(reader);

    if (CORBA::is_nil(reader_i.in())) {
        ACE_ERROR((LM_ERROR,
            ACE_TEXT("ERROR: %N:%l: on_data_available() -")
            ACE_TEXT(" _narrow failed!\n")));
        ACE_OS::exit(-1);
    }

    MessageType message(new PsBench::DeviceMessage);
    DDS::SampleInfo info;

    DDS::ReturnCode_t error = reader_i->take_next_sample(*message, info);

    if (error == DDS::RETCODE_OK) {

        if (info.valid_data)
        {
            DDS::Time_t recv_time = TheServiceParticipant->time_source().dds_time_t_now();
            unsigned long long fullRecvNs = ((unsigned long long)(recv_time.sec) * 1000000000) + (unsigned long long)recv_time.nanosec;

            DDS::Time_t pub_time = info.source_timestamp;
            unsigned long long fullPubNs = ((unsigned long long)(pub_time.sec) * 1000000000) + (unsigned long long)pub_time.nanosec;

            ERL_NIF_TERM recvTimeNs = enif_make_uint64(_env, fullRecvNs);
            ERL_NIF_TERM pubTimeNs = enif_make_uint64(_env, fullPubNs);
            ERL_NIF_TERM payloadSize = enif_make_uint(_env, message->payload.length());
            ERL_NIF_TERM seqId = enif_make_uint64(_env, message->seq_id);
            ERL_NIF_TERM publisherId = enif_make_string(_env, message->publisher_id, ERL_NIF_UTF8);
            
            ERL_NIF_TERM data_tuple = enif_make_tuple7(_env, _name, _topicName, seqId, pubTimeNs, recvTimeNs, payloadSize, publisherId);
            ERL_NIF_TERM send_tuple = enif_make_tuple2(_env, _recv_atom, data_tuple);

            enif_send(NULL, _listenerPid, NULL, send_tuple);
        }
    } else {
        ACE_ERROR((LM_ERROR,
            ACE_TEXT("ERROR: %N:%l: on_data_available() -")
            ACE_TEXT(" take_next_sample failed!\n")));
    }
}

void
    DataReaderListenerImpl::on_subscription_matched(
    DDS::DataReader_ptr /*reader*/,
    const DDS::SubscriptionMatchedStatus& /*status*/)
{
}

void
    DataReaderListenerImpl::on_sample_lost(
    DDS::DataReader_ptr /*reader*/,
    const DDS::SampleLostStatus& /*status*/)
{
}