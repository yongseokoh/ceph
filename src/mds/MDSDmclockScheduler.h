// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 LINE
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef MDS_DMCLOCK_SCHEDULER_H_
#define MDS_DMCLOCK_SCHEDULER_H_

#include <string>
#include <chrono>
#include <functional>
#include <map>
#include <mutex>
#include <deque>

#include "include/types.h"
#include "mdstypes.h"

#include "MDSRank.h"
#include "messages/MClientReply.h"
#include "messages/MClientRequest.h"
#include "messages/MClientSession.h"
#include "messages/MDSDmclockQoS.h"
#include "msg/Messenger.h"
#include "dmclock/src/dmclock_server.h"
#include "CInode.h"

using MDSReqRef = MClientRequest::const_ref;
using crimson::dmclock::ClientInfo;
using crimson::dmclock::AtLimit;
using crimson::dmclock::PhaseType;
using crimson::dmclock::ReqParams;
using Time = double;
using ClientId = std::string;
using VolumeId = ClientId;

constexpr std::string_view ROOT_VOLUME_ID = "/";

enum class RequestType {
  CLIENT_REQUEST,
  UPDATE_REQUEST
};

class Request {
private:
  RequestType type;
  VolumeId volume_id;

public:
  explicit Request(RequestType _type, VolumeId _volume_id) :
    type(_type), volume_id(_volume_id) {};

  RequestType get_request_type() const
  {
    return type;
  }

  const VolumeId& get_volume_id() const
  {
    return volume_id;
  }
};

class ClientRequest : public Request {
public:
  const MDSReqRef mds_req_ref;
  Time time;
  uint32_t cost;
  explicit ClientRequest(const MDSReqRef &_mds_req_ref, VolumeId _id,
      double _time, uint32_t _cost) :
      Request(RequestType::CLIENT_REQUEST, _id),
      mds_req_ref(_mds_req_ref), time(_time), cost(_cost) {};
};

class UpdateRequest : public Request {
public:
  UpdateRequest(VolumeId _id):
    Request(RequestType::UPDATE_REQUEST, _id) {};
};

class QoSInfo : public ClientInfo {
public:
  explicit QoSInfo(double reservation, double weight, double limit) :
    ClientInfo(reservation, weight, limit) {};

  void set_reservation(double reservation)
  {
    reservation = reservation;
    reservation_inv = 1.0 / reservation;
  }

  void set_weight(double weight)
  {
    weight = weight;
    weight_inv = 1.0 / weight;
  }

  void set_limit(double limit)
  {
    limit = limit;
    limit_inv = 1.0 / limit;
  }

  double get_reservation() const
  {
    return reservation;
  }

  double get_weight() const
  {
    return weight;
  }

  double get_limit() const
  {
    return limit;
  }

  const ClientInfo* get_qos_info() const
  {
    return this;
  }
};

class VolumeInfo : public QoSInfo {
private:
  int32_t session_cnt;
  bool use_default;

public:
  explicit VolumeInfo(double reservation, double weight, double limit, bool _use_default): 
    QoSInfo(reservation, weight, limit), session_cnt(1), use_default(_use_default) {};

  void inc_ref_cnt() { session_cnt++; };
  void dec_ref_cnt() { session_cnt--; };
  int32_t get_ref_cnt() { return session_cnt; };
  
  bool is_use_default()
  {
    return use_default;
  }
  void set_use_default(bool _use_default)
  {
    use_default = _use_default;
  }

  void update_volume_info(double reservation, double weight, double limit, bool use_default)
  {
    set_reservation(reservation);
    set_weight(weight);
    set_limit(limit);
    set_use_default(use_default);
  };
};

ostream& operator<<(ostream& os, const VolumeInfo* vi);

class mds_dmclock_conf : public QoSInfo {
private:
  bool enabled;

public:
  mds_dmclock_conf(): QoSInfo(0.0, 0.0, 0.0), enabled(false){};

  bool get_status()
  {
    return enabled;
  }

  bool is_enabled()
  {
    return enabled;
  }

  void set_status(bool _enabled)
  {
    enabled = _enabled;
  }
};

enum class SchedulerState {
  INIT,
  RUNNING,
  FINISHING,
  SHUTDOWN,
};

class MDSDmclockScheduler {
private:
  mds_dmclock_conf default_conf;
  SchedulerState state;
  MDSRank *mds;
  using Queue = crimson::dmclock::PushPriorityQueue<VolumeId, ClientRequest>;
  Queue *dmclock_queue;
  std::map<VolumeId, VolumeInfo> volume_info_map;

public:

  mds_dmclock_conf get_default_conf()
  {
    return default_conf;
  }

  /* volume QoS info management */
  void create_volume_info(const VolumeId &id, double reservation, double weight, double limit, bool use_default);
  void update_volume_info(const VolumeId &id, double reservation, double weight, double limit, bool use_default);
  VolumeInfo *get_volume_info(const VolumeId &id);
  void delete_volume_info(const VolumeId &id);
  void set_default_volume_info(const VolumeId &id);
  void dump_volume_info(Formatter *f) const;
  void create_qos_info_from_xattr(const VolumeId &id);
  void update_qos_info_from_xattr(const VolumeId &id);
  void delete_qos_info_by_session(Session *session);

  /* multi MDS broadcast message */
  void broadcast_qos_info_update_to_mds(const VolumeId& id);
  void handle_qos_info_update_message(const MDSDmclockQoS::const_ref &m);
  void proc_message(const Message::const_ref &m);

  void handle_mds_request(const MDSReqRef &req);
  void submit_request_to_mds(const VolumeId &, std::unique_ptr<ClientRequest> &&, const PhaseType&, const uint64_t);
  const ClientInfo *get_client_info(const VolumeId &id);

  void handle_conf_change(const std::set<std::string>& changed);

  void enable_qos_feature();
  void disable_qos_feature();

  CInode *read_xattrs(const VolumeId id);

  /* request event handler */
  void begin_schedule_thread();
  void process_request();
  std::thread scheduler_thread;
  mutable std::mutex queue_mutex;
  std::condition_variable queue_cvar;

  void invoke_request_completed();
  bool need_request_completed;

  std::deque<std::unique_ptr<Request>> request_queue;
  void enqueue_update_request(const VolumeId& id);
  uint32_t get_request_queue_size();

  const VolumeId& get_volume_id(Session *session);

  using RejectThreshold = Time;
  using AtLimitParam = boost::variant<AtLimit, RejectThreshold>;

  explicit MDSDmclockScheduler(MDSRank *m) : mds(m)
  {
    dmclock_queue = new Queue(
        std::bind(&MDSDmclockScheduler::get_client_info, this, std::placeholders::_1),
        {[]()->bool{ return true;}},
        std::bind(&MDSDmclockScheduler::submit_request_to_mds, this, std::placeholders::_1,
          std::placeholders::_2, std::placeholders::_3, std::placeholders::_4));

    state = SchedulerState::RUNNING;

    begin_schedule_thread();

    default_conf.set_reservation(g_conf().get_val<double>("mds_dmclock_mds_qos_default_reservation"));
    default_conf.set_weight(g_conf().get_val<double>("mds_dmclock_mds_qos_default_weight"));
    default_conf.set_limit(g_conf().get_val<double>("mds_dmclock_mds_qos_default_limit"));
    default_conf.set_status(g_conf().get_val<bool>("mds_dmclock_mds_qos_enable"));

    ceph_assert(ROOT_VOLUME_ID == "/");
  }
  ~MDSDmclockScheduler();

  void shutdown();
  friend ostream& operator<<(ostream& os, const VolumeInfo* vi);
};

#endif // MDS_DMCLOCK_SCHEDULER_H_
