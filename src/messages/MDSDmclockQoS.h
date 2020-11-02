// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_MDSDMCLOCKQOS_H
#define CEPH_MDSDMCLOCKQOS_H

#include "include/types.h"
#include "msg/Message.h"

#define MASTER_VERSION

#ifdef MASTER_VERSION
#include "messages/MMDSOp.h"
#endif

#ifndef MASTER_VERSION
class MDSDmclockQoS : public MessageInstance<MDSDmclockQoS> {
#else
class MDSDmclockQoS : public MMDSOp {
  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;
#endif
public:

#ifndef MASTER_VERSION
  friend factory;
#endif

private:
  std::string volume_id;

 public:
  std::string get_volume_id() const { return volume_id; }

protected:
#ifndef MASTER_VERSION
  MDSDmclockQoS() : MessageInstance(MSG_MDS_DMCLOCK_QOS) {}
#else
  MDSDmclockQoS() : MMDSOp{MSG_MDS_FINDINO, HEAD_VERSION, COMPAT_VERSION} {}
#endif
#ifndef MASTER_VERSION
  MDSDmclockQoS(const std::string& _volume_id) : MessageInstance(MSG_MDS_DMCLOCK_QOS) {
    this->volume_id= _volume_id;
  }
#else
  MDSDmclockQoS(const std::string& _volume_id) : MMDSOp{MSG_MDS_FINDINO, HEAD_VERSION, COMPAT_VERSION}, volume_id(_volume_id) {}
#endif
  ~MDSDmclockQoS() override {}

public:
  std::string_view get_type_name() const override { return "mds_dmlock_qos"; }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(volume_id, payload);
  }
  void decode_payload() override {
    auto p = payload.cbegin();
    decode(volume_id, p);
  }

#ifdef MASTER_VERSION
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
#endif
};

#endif
