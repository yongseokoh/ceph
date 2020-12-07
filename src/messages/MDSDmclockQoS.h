// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_MDSDMCLOCKQOS_H
#define CEPH_MDSDMCLOCKQOS_H

#include "include/types.h"
#include "msg/Message.h"

class MDSDmclockQoS : public MessageInstance<MDSDmclockQoS> {
public:
  friend factory;
private:
  std::string volume_id;

 public:
  std::string get_volume_id() const { return volume_id; }

protected:
  MDSDmclockQoS() : MessageInstance(MSG_MDS_DMCLOCK_QOS) {}
  MDSDmclockQoS(const std::string& _volume_id) : MessageInstance(MSG_MDS_DMCLOCK_QOS) {
    this->volume_id= _volume_id;
  }
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

};

#endif
