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

#include "SessionMap.h"
#include "MDSDmclockScheduler.h"
#include "mds/MDSMap.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << get_nodeid() << ".dmclock_scheduler "

const VolumeId &MDSDmclockScheduler::get_volume_id(Session *session)
{
  ceph_assert(session != nullptr);

  auto client_root_entry = session->info.client_metadata.find("root");

  ceph_assert(client_root_entry != session->info.client_metadata.end());

  return client_root_entry->second;
}

const VolumeId MDSDmclockScheduler::get_session_id(Session *session)
{
  ceph_assert(session != nullptr);
  return to_string(session->info.inst.name.num());
}

template<typename R>
void MDSDmclockScheduler::enqueue_client_request(const R &mds_req, VolumeId volume_id)
{
    std::unique_lock<std::mutex> lock(queue_mutex);
    request_queue.emplace_back(new ClientRequest(mds_req, volume_id, crimson::dmclock::get_time(), 1));
    /* wake up*/
    lock.unlock();
    queue_cvar.notify_all();
}

void MDSDmclockScheduler::handle_mds_request(const MDSReqRef &mds_req)
{
  ceph_assert(mds_is_locked_by_me());

  auto volume_id = get_volume_id(mds->get_session(mds_req));

  if (mds->mds_dmclock_scheduler->default_conf.is_enabled() == true) {
    dout(0) << "add request to add thread " << volume_id << dendl;
    enqueue_client_request<MDSReqRef>(mds_req, volume_id);
  } else {
    dout(0) << "add request to mds " << dendl;
    mds->server->handle_client_request(mds_req);
  }
}

void MDSDmclockScheduler::submit_request_to_mds(const VolumeId& vid, std::unique_ptr<ClientRequest>&& request, const PhaseType& phase_type, const uint64_t cost)
{
  dout(0) << "submit_request_to_mds(): volume vid " << vid << dendl;

  const MDSReqRef& req = request->mds_req_ref;

  ceph_assert(!mds_is_locked_by_me());

  mds_lock();

  mds->server->handle_client_request(req);

  mds_unlock();
}

void MDSDmclockScheduler::shutdown()
{
  if (default_conf.is_enabled() == true) {
    disable_qos_feature();
  }

  std::unique_lock<std::mutex> lock(queue_mutex);
  state = SchedulerState::FINISHING;
  lock.unlock();
  queue_cvar.notify_all();

  if (scheduler_thread.joinable()) {
    scheduler_thread.join();
  }

  state = SchedulerState::SHUTDOWN;

  dout(0) << "MDSDmclockScheduler::shutdown()" << dendl;
}

MDSDmclockScheduler::~MDSDmclockScheduler()
{
  shutdown();
  delete dmclock_queue;
}

const ClientInfo *MDSDmclockScheduler::get_client_info(const VolumeId &vid)
{
  auto vi = get_volume_info(vid);
  const ClientInfo *ci = nullptr;
  if (vi != nullptr) {
    if (vi->is_use_default() == true) {
      dout(0) << "get_client_info with default " << *default_conf.get_qos_info() << dendl;
      ci = default_conf.get_qos_info();
    } else {
      dout(0) << "get_client_info with per client specific " << vi->get_qos_info() << dendl;
      ci = vi->get_qos_info();
    }
  }
  if (ci) {
    dout(0) << "debug " << *ci << dendl;
  }
  return ci;
}

void MDSDmclockScheduler::dump_volume_info(Formatter *f) const
{
  f->open_array_section("volume_infos");
  for (auto it = volume_info_map.begin(); it != volume_info_map.end(); it++) {
    auto vol_info = it->second;

    f->open_object_section("volume_info");
    f->dump_string("volume_id", it->first);
    f->dump_bool("use_default", vol_info.is_use_default());

    if (vol_info.is_use_default() == true) {
      f->dump_float("reservation", default_conf.get_reservation());
      f->dump_float("weight", default_conf.get_weight());
      f->dump_float("limit", default_conf.get_limit());
    } else {
      f->dump_float("reservation", vol_info.get_reservation());
      f->dump_float("weight", vol_info.get_weight());
      f->dump_float("limit", vol_info.get_limit());
    }
    f->dump_int("session_cnt", vol_info.get_session_cnt());
    f->close_section();
  }
  f->close_section();
}

VolumeInfo *MDSDmclockScheduler::get_volume_info(const VolumeId &vid)
{
  auto it = volume_info_map.find(vid);
  if (it != volume_info_map.end()) {
    return &it->second;
  }
  return nullptr;
}

void MDSDmclockScheduler::create_volume_info(const VolumeId &vid, const double reservation, const double weight, const double limit, const bool use_default)
{
  VolumeInfo* vi = get_volume_info(vid);

  dout(0) << "create_volume_info() reservation = " << reservation << " weight " << weight <<
    " limit " << limit << " use_default " << use_default<< dendl;

  if (vi == nullptr) {
    auto [it, success]  = volume_info_map.insert(std::make_pair(std::move(vid), std::move(VolumeInfo())));
    ceph_assert(success==true);
    vi = &it->second;
  }
  vi->update_volume_info(reservation, weight, limit, use_default);

  enqueue_update_request(vid);
}

void MDSDmclockScheduler::add_session_to_volume_info(const VolumeId &vid, const SessionId &sid)
{
  dout(1) << __func__ << dendl;

  VolumeInfo* vi = get_volume_info(vid);
  if (vi) {
    vi->add_session(sid);
  }
}

void MDSDmclockScheduler::delete_session_from_volume_info(const VolumeId &vid, const SessionId &sid)
{
  auto it = volume_info_map.find(vid);
  if (it != volume_info_map.end()) {
    auto vi = &it->second;
    //vi->dec_ref_cnt();
    vi->remove_session(sid);
    dout(0) << "delete volume info " << vi->get_session_cnt() << dendl;
    if (vi->get_session_cnt() == 0) {
      volume_info_map.erase(it);
    }
    /*TODO: delete client info in ClientRec */
  }
}

void MDSDmclockScheduler::update_volume_info(const VolumeId &vid, double reservation, double weight, double limit, bool use_default)
{
  dout(0) << "update_volume_info" << 
             " reservation " << reservation <<
             " weight " << weight << 
             " limit " << limit << 
             " use_default " << use_default << dendl;
  
  VolumeInfo* vi = get_volume_info(vid);
  if (vi) {
    vi->update_volume_info(reservation, weight, limit, use_default);
    enqueue_update_request(vid);
  }
}

void MDSDmclockScheduler::set_default_volume_info(const VolumeId &vid)
{
  dout(0) << "set_default_volume_info vid " << vid  << dendl;

  update_volume_info(vid, 0.0, 0.0, 0.0, true);
}

ostream& operator<<(ostream& os, VolumeInfo* vi)
{
  os << "VolumeInfo: (session_cnt " << vi->get_session_cnt() << ") "<< "reservation=" << vi->get_reservation() << " weight=" <<
    vi->get_weight() << " limit=" << vi->get_limit();
  return os;
}

void MDSDmclockScheduler::create_qos_info_from_xattr(Session *session)
{
  if (session == nullptr) {
    return;
  }

  VolumeId vid = get_volume_id(session);
  SessionId sid = get_session_id(session);

  dout(0) << "create_qos_info_from_xattr() root = " << vid<<  dendl;

  if (get_volume_info(vid) == nullptr) {
    CInode *in = read_xattrs(vid);
    auto pip = in->get_projected_inode();

    bool qos_valid = (in &&
                      pip->dmclock_info.mds_reservation > 0.0 &&
                      pip->dmclock_info.mds_weight > 0.0 &&
                      pip->dmclock_info.mds_limit > 0.0);

    double reservation;
    double weight;
    double limit;
    bool use_default;

    if (in && qos_valid) {
      reservation = pip->dmclock_info.mds_reservation;
      weight = pip->dmclock_info.mds_weight;
      limit = pip->dmclock_info.mds_limit;
      use_default = false;
    } else {
      reservation = 0.0;
      weight = 0.0;
      limit = 0.0;
      use_default = true;
    }

    create_volume_info(vid, reservation, weight, limit, use_default);
  } 
  add_session_to_volume_info(vid, sid);
}

void MDSDmclockScheduler::update_qos_info_from_xattr(const VolumeId &vid)
{
  dout(0) << "update_qos_info_from_xattr() root = " << vid <<  dendl;

  if (get_volume_info(vid) == nullptr) {
    return;
  }

  CInode *in = read_xattrs(vid);
  auto pip = in->get_projected_inode();

  bool qos_valid = (in &&
                    pip->dmclock_info.mds_reservation > 0.0 &&
		    pip->dmclock_info.mds_weight > 0.0 &&
		    pip->dmclock_info.mds_limit > 0.0);

  double reservation;
  double weight;
  double limit;
  bool use_default;

  if (in && qos_valid) {
    reservation = pip->dmclock_info.mds_reservation;
    weight = pip->dmclock_info.mds_weight;
    limit = pip->dmclock_info.mds_limit;
    use_default = false;
  } else {
    reservation = 0.0;
    weight = 0.0;
    limit = 0.0;
    use_default = true;
  }

  update_volume_info(vid, reservation, weight, limit, use_default);
}


void MDSDmclockScheduler::delete_qos_info_by_session(Session *session)
{
  VolumeId vid = get_volume_id(session);
  SessionId sid = get_session_id(session);
  dout(0) << "delete qos info when session is closed (volume_id " << vid << ")" << dendl;
  delete_session_from_volume_info(vid, sid);
}

void MDSDmclockScheduler::broadcast_qos_info_update_to_mds(const VolumeId& vid)
{
  std::set<mds_rank_t> actives;
  mds->get_mds_map()->get_active_mds_set(actives);

  dout(0) << "broadcast_qos_info_update_to_mds()" << dendl;
  dout(0) << "i am mds " << mds->get_nodeid() << dendl;

  for (auto it : actives) {
    dout(0) << "active MDS " << it << dendl;
    if (mds->get_nodeid() == it) {
      continue;
    }
    auto qos_msg = make_message<MDSDmclockQoS>(vid);

    mds->send_message_mds(qos_msg, it);
    dout(0) << "send message to MDS " << it << " vid: " << vid << dendl;
  }
}

void MDSDmclockScheduler::handle_qos_info_update_message(const cref_t<MDSDmclockQoS> &m)
{
  assert(mds_is_locked_by_me());

  dout(0) << "handle_qos_info_update_message()" << dendl;
  dout(0) << "receive send_message in " << mds->get_nodeid() << " volume_id " << m->get_volume_id() << dendl;

  VolumeInfo* vi = get_volume_info(m->get_volume_id());
  if (vi != nullptr) {
    dout(0) << "session maintains client info for volume_id = " << m->get_volume_id() << dendl;
    update_qos_info_from_xattr(m->get_volume_id());
  } else {
    dout(0) << "there is not client info for volume_id = " << m->get_volume_id() << dendl;
  }
}

void MDSDmclockScheduler::proc_message(const cref_t<Message> &m)
{
  switch (m->get_type()) {
    case MSG_MDS_DMCLOCK_QOS:
      handle_qos_info_update_message(ref_cast<MDSDmclockQoS>(m));
      break;
  default:
    derr << " dmClock QoS unknown message " << m->get_type() << dendl_impl;
    ceph_abort_msg("dmClock QoS unknown message");
  }
}

uint32_t MDSDmclockScheduler::get_request_queue_size()
{
  std::unique_lock<std::mutex> lock(queue_mutex);

  return request_queue.size();
}

void MDSDmclockScheduler::enqueue_update_request(const VolumeId& vid)
{
    std::unique_lock<std::mutex> lock(queue_mutex);

    dout(0) << "Add update request volumeid " << vid << dendl;
    request_queue.emplace_back(new UpdateRequest(vid));

    /* wake up*/
    lock.unlock();
    queue_cvar.notify_all();
}

void MDSDmclockScheduler::enqueue_update_request(const VolumeId& vid, RequestCB cb_func)
{
    std::unique_lock<std::mutex> lock(queue_mutex);

    dout(0) << "Add update request volumeid " << vid << dendl;
    request_queue.emplace_back(new UpdateRequest(vid, cb_func));

    /* wake up*/
    lock.unlock();
    queue_cvar.notify_all();
}

void MDSDmclockScheduler::invoke_request_completed()
{
  if (default_conf.is_enabled() == false) {
    return;
  }

  std::unique_lock<std::mutex> lock(queue_mutex);
  dout(0) << __func__ << dendl;
  need_request_completed = true;
  queue_cvar.notify_all();
}

void MDSDmclockScheduler::process_request()
{
    std::unique_lock<std::mutex> lock(queue_mutex);

    while (state == SchedulerState::RUNNING) {
      queue_cvar.wait(lock);

      while (request_queue.size()) {
        std::unique_ptr<Request> request = std::move(request_queue.front());
        request_queue.erase(request_queue.begin());

        lock.unlock();

        switch(request->get_request_type()) {
          case RequestType::CLIENT_REQUEST:
          {
            std::unique_ptr<ClientRequest> c_request(static_cast<ClientRequest *>(request.release()));
            dout(0) << "Pop client request (size " << request_queue.size() << " volume_id "
                << c_request->get_volume_id() << " time " << c_request->time << " cost " << c_request->cost << ")" << dendl;

            auto r = dmclock_queue->add_request(std::move(c_request), std::move(c_request->get_volume_id()),
                {0, 0}, c_request->time, c_request->cost);

            dout(0) << "add request r value = " << r << dendl;
            break;
          }
          case RequestType::UPDATE_REQUEST:
          {
            std::unique_ptr<UpdateRequest> c_request(static_cast<UpdateRequest *>(request.release()));
            dout(0) << "Pop update request (size " << request_queue.size() << " volume_id "
                << c_request->get_volume_id() << ")" << dendl;
            dmclock_queue->update_client_info(c_request->get_volume_id());

            if (c_request->cb_func) {
              c_request->cb_func();
            }
            break;
          }
        }

        lock.lock();
      }

      if (need_request_completed == true) {
        need_request_completed = false;
        lock.unlock();

        dmclock_queue->request_completed();
        dout(0) << "Pop request completed request "<< dendl;

        lock.lock();
      }
    }
    dout(0) << "scheduler_thread has been stopped." << dendl;
}

void MDSDmclockScheduler::begin_schedule_thread()
{
  need_request_completed = false;

  scheduler_thread = std::thread([this](){process_request();});
}

CInode *MDSDmclockScheduler::read_xattrs(const VolumeId vid)
{
  CInode *in;

  filepath path_(vid.c_str());
  auto qos_msg = make_message<MDSDmclockQoS>(vid);
  CF_MDS_RetryMessageFactory cf(mds, qos_msg);

  // Do not need to set qos at ROOT_VOLUME_ID
  if (vid != ROOT_VOLUME_ID) {
    CInode *cur = mds->mdcache->get_inode(path_.get_ino());  //get base_ino
    
    mds->mdcache->discover_path(cur, CEPH_NOSNAP, path_, NULL, false);	// must discover
    if (mds->logger) mds->logger->inc(l_mds_traverse_discover);
  }

  MDRequestRef null_ref;

  int flags = MDS_TRAVERSE_DISCOVER;
  mds->mdcache->path_traverse(null_ref, cf, path_, flags, NULL, &in);

  auto pip = in->get_projected_inode();

  dout(20) << "read_xattrs: found inode: " << pip << " version " << pip->version <<  dendl;
  dout(20) << "dmclock_info, reservation: " << pip->dmclock_info.mds_reservation 
    << " weight: " << pip->dmclock_info.mds_weight << " limit: " << pip->dmclock_info.mds_limit << dendl;

  return in;
}

void MDSDmclockScheduler::enable_qos_feature()
{
  dout(0) << "enable_qos_feature()" << dendl;

  default_conf.set_status(true);

  auto sessionmap = get_session_map();

  if (sessionmap == nullptr) {
    return;
  }

  if (auto it = sessionmap->by_state.find(Session::STATE_OPEN); it != sessionmap->by_state.end()) {
    for (const auto &session : *(it->second)) {
      create_qos_info_from_xattr(session);
    }
  }
  if (auto it = sessionmap->by_state.find(Session::STATE_STALE); it != sessionmap->by_state.end()) {
    for (const auto &session : *(it->second)) {
      create_qos_info_from_xattr(session);
    }
  }
}

void MDSDmclockScheduler::disable_qos_feature()
{
  uint32_t queue_size;
  bool dmclock_empty;

  dout(0) << "disable_qos_feature()" << dendl;

  do
  {
    mds_unlock();

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    queue_size = get_request_queue_size();
    dmclock_empty = dmclock_queue->empty();

    mds_lock();
  } while(queue_size || !dmclock_empty);

  default_conf.set_status(false);

  auto sessionmap = get_session_map();

  if (sessionmap == nullptr) {
    return;
  }

  if (auto it = sessionmap->by_state.find(Session::STATE_OPEN); it != sessionmap->by_state.end()) {
    for (const auto &session : *(it->second)) {
      delete_qos_info_by_session(session);
    }
  }

  if (auto it = sessionmap->by_state.find(Session::STATE_STALE); it != sessionmap->by_state.end()) {
    for (const auto &session : *(it->second)) {
      delete_qos_info_by_session(session);
    }
  }
}

void MDSDmclockScheduler::handle_conf_change(const std::set<std::string>& changed)
{
  dout(0) << "MDSDmclockScheduler::handle_conf_change() called" << dendl;

  if (changed.count("mds_dmclock_mds_qos_enable")) {
    bool new_val = g_conf().get_val<bool>("mds_dmclock_mds_qos_enable");
    if (default_conf.is_enabled() != new_val)
    {
      if (new_val == true) {
        enable_qos_feature();
      } else {
        disable_qos_feature();
      }
    }
  }

  if (changed.count("mds_dmclock_mds_qos_default_reservation") || default_conf.is_enabled() == true) {
    default_conf.set_reservation(g_conf().get_val<double>("mds_dmclock_mds_qos_default_reservation"));
    dout(0) << " set reservation " << g_conf().get_val<double>("mds_dmclock_mds_qos_default_reservation") << dendl;
    ceph_assert(default_conf.get_reservation() == g_conf().get_val<double>("mds_dmclock_mds_qos_default_reservation"));
  }
  if (changed.count("mds_dmclock_mds_qos_default_weight") || default_conf.is_enabled() == true) {
    default_conf.set_weight(g_conf().get_val<double>("mds_dmclock_mds_qos_default_weight"));
    dout(0) << " set weight " << g_conf().get_val<double>("mds_dmclock_mds_qos_default_weight") << dendl;
    ceph_assert(default_conf.get_weight() == g_conf().get_val<double>("mds_dmclock_mds_qos_default_weight"));
  }
  if (changed.count("mds_dmclock_mds_qos_default_limit") || default_conf.is_enabled() == true) {
    default_conf.set_limit(g_conf().get_val<double>("mds_dmclock_mds_qos_default_limit"));
    dout(0) << " set limit " << g_conf().get_val<double>("mds_dmclock_mds_qos_default_limit") << dendl;
    ceph_assert(default_conf.get_limit() == g_conf().get_val<double>("mds_dmclock_mds_qos_default_limit"));
  }

  /* need to check whether conf is updated from ceph.conf when the MDS is restarted */
  dout(0) << "handle_conf_change() enable=" << default_conf.is_enabled() << " reservation=" << default_conf.get_reservation() << " weight=" 
    << default_conf.get_weight() << " limit=" << default_conf.get_limit() << dendl;
}

mds_rank_t MDSDmclockScheduler::get_nodeid()
{
  if (mds != nullptr) {
    return mds->get_nodeid();
  }
  return 0;
}

SessionMap *MDSDmclockScheduler::get_session_map()
{
  if (mds != nullptr) {
    return &mds->sessionmap;
  }
  return 0;
}

void MDSDmclockScheduler::mds_lock()
{
  if (mds != nullptr) {
    mds->mds_lock.lock();
  }
}

void MDSDmclockScheduler::mds_unlock()
{
  if (mds != nullptr) {
    mds->mds_lock.unlock();
  }
}

int MDSDmclockScheduler::mds_is_locked_by_me()
{
  if (mds != nullptr) {
    return mds->mds_lock.is_locked_by_me();
  }
  return 1;
}
