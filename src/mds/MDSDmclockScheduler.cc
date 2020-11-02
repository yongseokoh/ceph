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
#define dout_prefix *_dout << "mds." << mds->get_nodeid() << ".dmclock_scheduler "

const VolumeId &MDSDmclockScheduler::get_volume_id(Session *session)
{
  auto client_root_entry = session->info.client_metadata.find("root");

  ceph_assert(client_root_entry != session->info.client_metadata.end());

  return client_root_entry->second;
}

#if 0
void MDSDmclockScheduler::handle_mds_request(const MDSReqRef &mds_req)
#else
void MDSDmclockScheduler::handle_mds_request(const cref_t<MClientRequest> &mds_req)
#endif
{
  ceph_assert(mds->mds_lock.is_locked_by_me());

  auto volume_id = get_volume_id(mds->get_session(mds_req));

  if (mds->mds_dmclock_scheduler->default_conf.is_enabled() == true && volume_id != ROOT_VOLUME_ID) {
    dout(0) << "add request to add thread " << volume_id << dendl;

    std::unique_lock<std::mutex> lock(queue_mutex);
    request_queue.emplace_back(new ClientRequest(mds_req, volume_id, crimson::dmclock::get_time(), 1));
    /* wake up*/
    lock.unlock();
    queue_cvar.notify_all();

  } else {
    dout(0) << "add request to mds " << dendl;
    mds->server->handle_client_request(mds_req);
  }
}

void MDSDmclockScheduler::submit_request_to_mds(const VolumeId& id, std::unique_ptr<ClientRequest>&& request, const PhaseType& phase_type, const uint64_t cost)
{
  dout(0) << "submit_request_to_mds(): volume id " << id << dendl;

  const MDSReqRef& req = request->mds_req_ref;

  ceph_assert(!mds->mds_lock.is_locked_by_me());

  mds->mds_lock.lock();

  mds->server->handle_client_request(req);

  mds->mds_lock.unlock();
}

void MDSDmclockScheduler::shutdown()
{
  std::unique_lock<std::mutex> lock(queue_mutex);

  if (default_conf.is_enabled() == true) {
    disable_qos_feature();
  }

  state = SchedulerState::FINISHING;

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

const ClientInfo *MDSDmclockScheduler::get_client_info(const VolumeId &id)
{
  auto vi = get_volume_info(id);
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
    f->dump_int("session_cnt", vol_info.get_ref_cnt());
    f->close_section();
  }
  f->close_section();
}

VolumeInfo *MDSDmclockScheduler::get_volume_info(const VolumeId &id)
{
  auto it = volume_info_map.find(id);
  if (it != volume_info_map.end()) {
    return &it->second;
  }
  return nullptr;
}

void MDSDmclockScheduler::create_volume_info(const VolumeId &id, const double reservation, const double weight, const double limit, const bool use_default)
{
  VolumeInfo* vi = get_volume_info(id);

  dout(0) << "create_volume_info() reservation = " << reservation << " weight " << weight << 
    " limit " << limit << " use_default " << use_default<< dendl;
  if (vi == nullptr) {
    volume_info_map.insert(std::make_pair(std::move(id), std::move(VolumeInfo(reservation, weight, limit, use_default))));
  } else {
    vi->inc_ref_cnt();
    vi->update_volume_info(reservation, weight, limit, use_default);
  }

  enqueue_update_request(id);
}

void MDSDmclockScheduler::delete_volume_info(const VolumeId &id)
{
  auto it = volume_info_map.find(id);
  if (it != volume_info_map.end()) {
    auto vi = &it->second;
    vi->dec_ref_cnt();
    dout(0) << "delete volume info " << vi->get_ref_cnt() << dendl;
    if (vi->get_ref_cnt() == 0) {
      volume_info_map.erase(it);
    }
    /*TODO: delete client info in ClientRec */
  }
}

void MDSDmclockScheduler::update_volume_info(const VolumeId &id, double reservation, double weight, double limit, bool use_default)
{
  dout(0) << "update_volume_info" << 
             " reservation " << reservation <<
             " weight " << weight << 
             " limit " << limit << 
             " use_default " << use_default << dendl;
  
  VolumeInfo* vi = get_volume_info(id);
  if (vi) {
    vi->update_volume_info(reservation, weight, limit, use_default);
    enqueue_update_request(id);
  }
}

void MDSDmclockScheduler::set_default_volume_info(const VolumeId &id)
{
  dout(0) << "set_default_volume_info id " << id  << dendl;

  update_volume_info(id, 0.0, 0.0, 0.0, true);
}

ostream& operator<<(ostream& os, VolumeInfo* vi)
{
  os << "VolumeInfo: (session_cnt " << vi->get_ref_cnt() << ") "<< "reservation=" << vi->get_reservation() << " weight=" <<
    vi->get_weight() << " limit=" << vi->get_limit();
  return os;
}

void MDSDmclockScheduler::create_qos_info_from_xattr(const VolumeId &id)
{

  dout(0) << "create_qos_info_from_xattr() root = " << id<<  dendl;

  CInode *in = read_xattrs(id);
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

  create_volume_info(id, reservation, weight, limit, use_default);
}

void MDSDmclockScheduler::update_qos_info_from_xattr(const VolumeId &id)
{

  dout(0) << "update_qos_info_from_xattr() root = " << id <<  dendl;

  CInode *in = read_xattrs(id);
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

  update_volume_info(id, reservation, weight, limit, use_default);
}


void MDSDmclockScheduler::delete_qos_info_by_session(Session *session)
{
  VolumeId id = get_volume_id(session);
  dout(0) << "delete qos info when session is closed (volume_id " << id << ")" << dendl;

  delete_volume_info(id);
}

void MDSDmclockScheduler::broadcast_qos_info_update_to_mds(const VolumeId& id)
{
  std::set<mds_rank_t> actives;
  mds->get_mds_map()->get_active_mds_set(actives);

  dout(0) << "broadcast_qos_info_update_to_mds()" << dendl;

  for (auto it : actives) {
    dout(0) << "active MDS " << it << dendl;
    if (mds->get_nodeid() == it) {
      continue;
    }
    dout(0) << "send message to MDS " << it << dendl;

    int r = 1;
#if 0
    auto qos_msg = MDSDmclockQoS::create(id);
#else
    auto qos_msg = make_message<MDSDmclockQoS>(id);
#endif
    /* root path  */
    if (mds->get_nodeid() == 0)
    {
      mds->send_message_mds(qos_msg, r);
      dout(0) << "send_message to rank: " << r << dendl;
    }
  }
}

void MDSDmclockScheduler::handle_qos_info_update_message(const cref_t<MDSDmclockQoS> &m)
{
  assert(mds->mds_lock.is_locked_by_me());

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
#if 0
      handle_qos_info_update_message(MDSDmclockQoS::msgref_cast(m));
#else
      handle_qos_info_update_message(ref_cast<MDSDmclockQoS>(m));
#endif
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

void MDSDmclockScheduler::enqueue_update_request(const VolumeId& id)
{
    std::unique_lock<std::mutex> lock(queue_mutex);

    dout(0) << "Add update request volumeid " << id << dendl;
    request_queue.emplace_back(new UpdateRequest(id));

    /* wake up*/
    lock.unlock();
    queue_cvar.notify_all();
}

void MDSDmclockScheduler::invoke_request_completed()
{
  std::unique_lock<std::mutex> lock(queue_mutex);
  dout(0) << __func__ << dendl;
  need_request_completed = true;
  queue_cvar.notify_all();
}

void MDSDmclockScheduler::process_request()
{
    std::unique_lock<std::mutex> lock(queue_mutex);

    while (state == SchedulerState::RUNNING) {
#if 1
      queue_cvar.wait(lock);
#else
      queue_cvar.wait_for(lock, 5 * 1s); 
#endif

      while (request_queue.size()) {
        std::unique_ptr<Request> request = std::move(request_queue.front());
        request_queue.erase(request_queue.begin());

        lock.unlock();

        switch(request->get_request_type()) {
          case RequestType::CLIENT_REQUEST:
          {
            std::unique_ptr<ClientRequest> c_request(static_cast<ClientRequest *>(request.release()));
            dout(0) << "Pop client request (size " << request_queue.size() << " volume_id "
                << c_request->get_volume_id() << " time " << c_request->time << " cost " << c_request->cost << dendl;
#if 1
            auto r = dmclock_queue->add_request(std::move(c_request), std::move(c_request->get_volume_id()),
                {0, 0}, c_request->time, c_request->cost);
#else
            auto r = dmclock_queue->add_request(std::move(c_request), c_request->get_volume_id(),
                {0, 0}, c_request->cost);
#endif
            dout(0) << "add request r value = " << r << dendl;
            dout(0) << "sched_ahead_when = " << setprecision(14) << dmclock_queue->get_sched_ahead_time() << dendl;
            dout(0) << "curtime = "  << setprecision(14) << crimson::dmclock::get_time() << dendl;
            dout(0) << "queue count = " << dmclock_queue->request_count() << dendl;
            break;
          }
          case RequestType::UPDATE_REQUEST:
          {
            std::unique_ptr<UpdateRequest> c_request(static_cast<UpdateRequest *>(request.release()));
            dout(0) << "Pop update request (size " << request_queue.size() << " volume_id "
                << c_request->get_volume_id() << dendl;
            dmclock_queue->update_client_info(c_request->get_volume_id());
            break;
          }
        }

        lock.lock();
      }

#if 0 
      dout(0) << "sched_ahead_when = " << std::setprecision(14) << dmclock_queue->get_sched_ahead_time() << dendl;
      dout(0) << "curtime = "  << std::setprecision(14) << crimson::dmclock::get_time() << dendl;
      dout(0) << "queue count = " << dmclock_queue->request_count() << dendl;
#endif
      dout(0) << " sched_at_count " << dmclock_queue->get_sched_at_count() << " get_submit_count " 
        << dmclock_queue->get_submit_count()
        << " wakeup count " << dmclock_queue->get_wakeup_count()
        << " wakeup2 count " << dmclock_queue->get_wakeup2_count()
        << dendl;

      //std::chrono::system_clock::time_point EndTime = std::chrono::system_clock::now();
      //dout(0) << "curtime = " << std::chrono::system_clock::now() << dendl;

       //dout(0) << std::chrono::system_clock::now() << dendl;
      if (0) 
      {
        using namespace std::chrono;

        duration<int,std::ratio<60*60*24> > one_day (1);

        system_clock::time_point today = system_clock::now();

        time_t tt;

        tt = system_clock::to_time_t ( today );
        dout(0) << "today is: " << ctime(&tt) << dendl;

        struct timespec now;
        auto result = clock_gettime(CLOCK_REALTIME, &now);
        (void) result; // reference result in case assert is compiled out
        assert(0 == result);
        dout(0) << "sec " << now.tv_sec << " nsec " << now.tv_nsec << dendl;
      }

#if 0
      if (need_request_completed == true || dmclock_queue->request_count()) {
#else
      if (need_request_completed == true) {
#endif
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

CInode *MDSDmclockScheduler::read_xattrs(const VolumeId id)
{
  CInode *in;

  filepath path_(id.c_str());
#if 0
  auto qos_msg = MDSDmclockQoS::create(id);
#else
  auto qos_msg = make_message<MDSDmclockQoS>(id);
#endif
  CF_MDS_RetryMessageFactory cf(mds, qos_msg);

  // Do not need to set qos at ROOT_VOLUME_ID
  if (id != ROOT_VOLUME_ID) {
    CInode *cur = mds->mdcache->get_inode(path_.get_ino());  //get base_ino
    
    mds->mdcache->discover_path(cur, CEPH_NOSNAP, path_, NULL, false);	// must discover
    if (mds->logger) mds->logger->inc(l_mds_traverse_discover);
  }

  MDRequestRef null_ref;
#if 0
  mds->mdcache->path_traverse(null_ref, cf, path_, NULL, &in, MDS_TRAVERSE_DISCOVER);
#else
  int flags = MDS_TRAVERSE_DISCOVER;
  mds->mdcache->path_traverse(null_ref, cf, path_, flags, NULL, &in);
#endif

  auto pip = in->get_projected_inode();

  dout(20) << "read_xattrs: found inode: " << pip << " version " << pip->version <<  dendl;
  dout(20) << "dmclock_info, reservation: " << pip->dmclock_info.mds_reservation 
    << " weight: " << pip->dmclock_info.mds_weight << " limit: " << pip->dmclock_info.mds_limit << dendl;

  return in;
}

void MDSDmclockScheduler::enable_qos_feature()
{
  auto sessionmap = &mds->sessionmap;

  dout(0) << "enable_qos_feature()" << dendl;

  default_conf.set_status(true);

  if (auto it = sessionmap->by_state.find(Session::STATE_OPEN); it != sessionmap->by_state.end()) {
    for (const auto &session : *(it->second)) {
      auto client_root_entry = session->info.client_metadata.find("root");
      ceph_assert(client_root_entry != session->info.client_metadata.end());
      create_qos_info_from_xattr(client_root_entry->second);
    }
  }
  if (auto it = sessionmap->by_state.find(Session::STATE_STALE); it != sessionmap->by_state.end()) {
    for (const auto &session : *(it->second)) {
      auto client_root_entry = session->info.client_metadata.find("root");
      ceph_assert(client_root_entry != session->info.client_metadata.end());
      create_qos_info_from_xattr(client_root_entry->second);
    }
  }
}

void MDSDmclockScheduler::disable_qos_feature()
{
  auto sessionmap = &mds->sessionmap;
  uint32_t queue_thread_count, dmclock_request_count;
  bool dmclock_empty;

  dout(0) << "disable_qos_feature()" << dendl;

  default_conf.set_status(false);

  do
  {
    mds->mds_lock.unlock();

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    invoke_request_completed();

    queue_thread_count = get_request_queue_size();
    dmclock_request_count = dmclock_queue->request_count();
    dmclock_empty = dmclock_queue->empty();

    dout(0) << "queue thread request " << queue_thread_count << " dmclock request " << dmclock_request_count << dendl;
    dout(0) << *dmclock_queue << dendl;

    mds->mds_lock.lock();
  } while(queue_thread_count || !dmclock_empty);

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

  ceph_assert(mds->mds_lock.is_locked());

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
