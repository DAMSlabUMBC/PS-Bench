#ifndef DATAREADER_LISTENER_IMPL_H
#define DATAREADER_LISTENER_IMPL_H

#include <ace/Global_Macros.h>
#include <dds/DdsDcpsSubscriptionS.h>
#include <dds/DCPS/LocalObject.h>
#include <dds/DCPS/Definitions.h>

#include <erl_nif.h>

#include <PsBenchTypeSupportC.h>
#include <PsBenchTypeSupportImpl.h>

#ifdef __GNUC__
#include <tr1/memory>
#endif

typedef std::tr1::shared_ptr<PsBench::DeviceMessage> MessageType;

class DataReaderListenerImpl
  : public virtual OpenDDS::DCPS::LocalObject<DDS::DataReaderListener> {


	ErlNifPid* _listenerPid;
  ErlNifEnv* _env;
  ERL_NIF_TERM _name;
  ERL_NIF_TERM _recv_atom;
  ERL_NIF_TERM _topicName;

public:
	DataReaderListenerImpl(ErlNifPid* listenerPid, char* clientName, char* topicName) 
    : _listenerPid(listenerPid)
    { 
      _env = enif_alloc_env(); 
      _recv_atom = enif_make_atom(_env, "publish_recv");
      _name = enif_make_string(_env, clientName, ERL_NIF_UTF8); 
      _topicName = enif_make_string(_env, topicName, ERL_NIF_UTF8); 
    }

  virtual void on_requested_deadline_missed(
    DDS::DataReader_ptr reader,
    const DDS::RequestedDeadlineMissedStatus& status);

  virtual void on_requested_incompatible_qos(
    DDS::DataReader_ptr reader,
    const DDS::RequestedIncompatibleQosStatus& status);

  virtual void on_sample_rejected(
    DDS::DataReader_ptr reader,
    const DDS::SampleRejectedStatus& status);

  virtual void on_liveliness_changed(
    DDS::DataReader_ptr reader,
    const DDS::LivelinessChangedStatus& status);

  virtual void on_data_available(
    DDS::DataReader_ptr reader);

  virtual void on_subscription_matched(
    DDS::DataReader_ptr reader,
    const DDS::SubscriptionMatchedStatus& status);

  virtual void on_sample_lost(
    DDS::DataReader_ptr reader,
    const DDS::SampleLostStatus& status);
};

#endif /* DATAREADER_LISTENER_IMPL_H */
