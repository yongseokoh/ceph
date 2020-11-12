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

#include <iostream>
#include "mds/MDSDmclockScheduler.h"
#include "gtest/gtest.h"

#define dout_subsys ceph_subsys_mds
#define dout_context g_ceph_context

MDSRank *mds= nullptr;

TEST(MDSDmclockScheduler, ConDecon)
{
  MDSDmclockScheduler *scheduler = new MDSDmclockScheduler(mds);
  delete scheduler;

  scheduler = new MDSDmclockScheduler(mds);
  scheduler->enable_qos_feature();
  scheduler->disable_qos_feature();
  delete scheduler;

  scheduler = new MDSDmclockScheduler(mds);
  scheduler->enable_qos_feature();
  delete scheduler;

  scheduler = new MDSDmclockScheduler(mds);
  scheduler->enable_qos_feature();
  scheduler->disable_qos_feature();
  scheduler->disable_qos_feature();
  delete scheduler;
}

TEST(MDSDmclockScheduler, ConfCheck)
{
  g_ceph_context->_conf.set_val("debug mds", "0/20");

  g_ceph_context->_conf.set_val("mds_dmclock_mds_qos_default_reservation", "10000");
  g_ceph_context->_conf.set_val("mds_dmclock_mds_qos_default_weight", "10000");
  g_ceph_context->_conf.set_val("mds_dmclock_mds_qos_default_limit", "10000");
  g_ceph_context->_conf.set_val("mds_dmclock_mds_qos_enable", "true");

  MDSDmclockScheduler *scheduler = new MDSDmclockScheduler(mds);

  ASSERT_TRUE(scheduler->get_default_conf().is_enabled()==true);
  ASSERT_TRUE(scheduler->get_default_conf().get_reservation()==10000);
  ASSERT_TRUE(scheduler->get_default_conf().get_weight()==10000);
  ASSERT_TRUE(scheduler->get_default_conf().get_limit()==10000);

  delete scheduler;

  g_ceph_context->_conf.set_val("mds_dmclock_mds_qos_enable", "false");

  scheduler = new MDSDmclockScheduler(mds);

  ASSERT_TRUE(scheduler->get_default_conf().is_enabled()==false);

  delete scheduler;
}

TEST(MDSDmclockScheduler, GoodBasic)
{
  MDSDmclockScheduler *scheduler = new MDSDmclockScheduler(mds);

  scheduler->enable_qos_feature();
  ASSERT_TRUE(scheduler->get_default_conf().is_enabled()==true);

  VolumeId vid = "/";
  VolumeInfo *vi;
  {
    double reservation = 10.0;
    double weight = 20.0;
    double limit = 30.0;
    bool use_default = false;

    scheduler->create_volume_info(vid, reservation, weight, limit, use_default);

    vi = scheduler->get_volume_info(vid);
    ASSERT_TRUE(vi->get_reservation() == reservation);
    ASSERT_TRUE(vi->get_weight() == weight);
    ASSERT_TRUE(vi->get_limit() == limit);
    ASSERT_TRUE(vi->is_use_default() == use_default);
  }

  {
    double reservation = 100.0;
    double weight = 200.0;
    double limit = 300.0;
    bool use_default = true;
    scheduler->update_volume_info(vid, reservation, weight, limit, use_default);

    ASSERT_TRUE(vi->get_reservation() == reservation);
    ASSERT_TRUE(vi->get_weight() == weight);
    ASSERT_TRUE(vi->get_limit() == limit);
    ASSERT_TRUE(vi->is_use_default() == use_default);
  }

  {
    double reservation = 100.0;
    double weight = 200.0;
    double limit = 300.0;
    bool use_default = false;
    scheduler->update_volume_info(vid, reservation, weight, limit, use_default);

  }

  {
    double reservation = 1000.0;
    double weight = 2000.0;
    double limit = 3000.0;
    bool use_default = false;

    vi->set_reservation(reservation);
    vi->set_weight(weight);
    vi->set_limit(limit);
    vi->set_use_default(use_default);

    ASSERT_TRUE(vi->get_reservation() == reservation);
    ASSERT_TRUE(vi->get_weight() == weight);
    ASSERT_TRUE(vi->get_limit() == limit);
    ASSERT_TRUE(vi->is_use_default() == use_default);
  }

  scheduler->set_default_volume_info(vid);
  ASSERT_TRUE(vi->is_use_default() == true);

  scheduler->delete_volume_info(vid);

  scheduler->disable_qos_feature();
  ASSERT_TRUE(scheduler->get_default_conf().is_enabled()==false);

  delete scheduler;
}

std::atomic_int request_count = 0;
std::atomic_int complete_count = 0;
std::atomic_int cancel_count = 0;

Queue::ClientInfoFunc client_info_f;
Queue::CanHandleRequestFunc can_handle_f;
Queue::HandleRequestFunc handle_request_f = [] /* &complete_count, &request_count*/
(const VolumeId& id, std::unique_ptr<ClientRequest>&& request, const PhaseType& phase_type, const uint64_t cost)
{
  complete_count++;
  dout(0) << "handle request count " << complete_count << "/" << request_count << dendl;
};

MDSDmclockScheduler *create_dmclock_scheduler()
{

  MDSDmclockScheduler *scheduler = new MDSDmclockScheduler(mds, client_info_f, can_handle_f, handle_request_f);

  request_count = 0;
  complete_count = 0;
  cancel_count = 0;
  return scheduler;
}

void cleanup_dmclock_scheduler(MDSDmclockScheduler *scheduler)
{
  delete scheduler;
}

TEST(MDSDmclockScheduler, IssueClientRequest)
{
  MDSDmclockScheduler *scheduler = create_dmclock_scheduler();
  scheduler->enable_qos_feature();

  VolumeId vid = "/";
  double reservation = 10.0;
  double weight = 20.0;
  double limit = 30.0;
  bool use_default = false;
  scheduler->create_volume_info(vid, reservation, weight, limit, use_default);

  for (int i = 0; i < 20; i++) {
    request_count++;
    MDSReqRef req;
    scheduler->enqueue_client_request<MDSReqRef>(req, vid);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  for (int i = 0; i < 20; i++) {
    request_count++;
    MDSReqRef req;
    scheduler->enqueue_client_request<MDSReqRef>(req, vid);

    if (i / 5 % 2 == 0) {
      scheduler->enable_qos_feature();
    } else {
      scheduler->disable_qos_feature();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  /* TODO: schedule() function needs to be triggered  */
  scheduler->disable_qos_feature();
  cleanup_dmclock_scheduler(scheduler);

  ASSERT_TRUE(request_count==complete_count);
}

TEST(MDSDmclockScheduler, CancelClientRequest)
{
  MDSDmclockScheduler *scheduler = create_dmclock_scheduler();
  scheduler->enable_qos_feature();

  VolumeId vid = "/";
  double reservation = 100.0;
  double weight = 200.0;
  double limit = 300.0;
  bool use_default = false;
  scheduler->create_volume_info(vid, reservation, weight, limit, use_default);

  for (int i = 0; i < 10; i++) {
    request_count++;
    MDSReqRef req;
    scheduler->enqueue_client_request<MDSReqRef>(req, vid);
  }

  std::list<Queue::RequestRef> req_list;
  auto accum_f = [&req_list] (Queue::RequestRef&& r)
                  {
                    req_list.push_front(std::move(r));
                    cancel_count++;
                    dout(0) << "cancel request " << cancel_count << dendl;
                  };

  /* TODO: sometimes client request are not at dmclock queue (request_queue -> dmclock queue) */
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));

  scheduler->get_dmclock_queue()->remove_by_client(vid, true, accum_f);

  scheduler->disable_qos_feature();
  cleanup_dmclock_scheduler(scheduler);

  ASSERT_TRUE(request_count==(complete_count+cancel_count));
}

TEST(MDSDmclockScheduler, IssueUpdateRequest)
{
  MDSDmclockScheduler *scheduler = create_dmclock_scheduler();
  scheduler->enable_qos_feature();
  atomic_int update_count = 0;

  VolumeId vid = "/";
  double reservation = 100.0;
  double weight = 200.0;
  double limit = 300.0;
  bool use_default = false;
  scheduler->create_volume_info(vid, reservation, weight, limit, use_default);

  std::mutex m;
  std::unique_lock<std::mutex> lk(m);
  std::condition_variable cv;

  RequestCB cb_func = [&update_count, &cv]
  {
    update_count++;
    cv.notify_one();
  };

  int sync_total_count = 100;
  for (int i = 0; i < sync_total_count; i++) {
    reservation = (std::rand() % 1000) + 1;
    weight = (std::rand() % 1000) + 1;
    limit = (std::rand() % 1000) + 1;
    scheduler->update_volume_info(vid, reservation, weight, limit, use_default);
    scheduler->enqueue_update_request(vid, cb_func);
    cv.wait(lk, []{return true;});
  }

  int async_total_count = 100;
  for (int i = 0; i < async_total_count; i++) {
    reservation = (std::rand() % 1000) + 1;
    weight = (std::rand() % 1000) + 1;
    limit = (std::rand() % 1000) + 1;
    scheduler->update_volume_info(vid, reservation, weight, limit, use_default);
    scheduler->enqueue_update_request(vid, cb_func);
  }

  scheduler->disable_qos_feature();
  cleanup_dmclock_scheduler(scheduler);
  ASSERT_TRUE(update_count==(sync_total_count+async_total_count));
}

TEST(MDSDmclockScheduler, IssueMixRequest)
{
  MDSDmclockScheduler *scheduler = create_dmclock_scheduler();
  scheduler->enable_qos_feature();
  atomic_int update_count = 0;

  VolumeId vid = "/";
  double reservation = 100.0;
  double weight = 200.0;
  double limit = 300.0;
  bool use_default = false;
  scheduler->create_volume_info(vid, reservation, weight, limit, use_default);

  std::mutex m;
  std::unique_lock<std::mutex> lk(m);
  std::condition_variable cv;

  RequestCB cb_func = [&update_count, &cv]
  {
    update_count++;
    cv.notify_one();
  };

  int sync_total_count = 100;
  for (int i = 0; i < sync_total_count; i++) {
    if (i % 2 == 0) {
      reservation = (std::rand() % 1000) + 1;
      weight = (std::rand() % 1000) + 1;
      limit = (std::rand() % 1000) + 1;
      scheduler->update_volume_info(vid, reservation, weight, limit, use_default);
      scheduler->enqueue_update_request(vid, cb_func);
      cv.wait(lk, []{return true;});
    } else {
      MDSReqRef req;
      scheduler->enqueue_client_request<MDSReqRef>(req, vid);
    }
  }

  scheduler->disable_qos_feature();
  cleanup_dmclock_scheduler(scheduler);
  ASSERT_TRUE((update_count+complete_count)==sync_total_count);
}
