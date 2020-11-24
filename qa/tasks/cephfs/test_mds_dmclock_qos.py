from tasks.cephfs.cephfs_test_case import CephFSTestCase
from teuthology.exceptions import CommandFailedError

import logging

log = logging.getLogger(__name__)

class TestMDSDmclockQoS(CephFSTestCase):

    CLIENTS_REQUIRED = 2
    MDSS_REQUIRED = 3
    REQUIRE_FILESYSTEM = True

    TEST_SUBVOLUME_PREFIX = "subvolume_"
    TEST_SUBVOLUME_COUNT = 2
    TEST_DIR_COUNT = 20
    subvolumes = []

    def setUp(self):
        super(TestMDSDmclockQoS, self).setUp()

        self.fs.set_max_mds(self.MDSS_REQUIRED)

        # create test subvolumes
        self.subvolumes = [self.TEST_SUBVOLUME_PREFIX + str(i)\
                for i in range(self.TEST_SUBVOLUME_COUNT)]

        # create subvolumes
        for subv_ in self.subvolumes:
            self._fs_cmd("subvolume", "create", self.fs.name, subv_)

        self.mount_a.umount_wait()
        self.mount_b.umount_wait()

        self.mount_a.mount(cephfs_mntpt=self.get_subvolume_path(self.subvolumes[0]))
        self.mount_b.mount(cephfs_mntpt=self.get_subvolume_path(self.subvolumes[1]))

        # chown root to user
        from os import getuid, getgid
        self.mount_a.run_shell(['sudo', 'chown', "{0}:{1}".format(getuid(), getgid()), self.mount_a.hostfs_mntpt])
        self.mount_b.run_shell(['sudo', 'chown', "{0}:{1}".format(getuid(), getgid()), self.mount_b.hostfs_mntpt])

    def _fs_cmd(self, *args):
        return self.mgr_cluster.mon_manager.raw_cluster_cmd("-c", "ceph.conf", "-k", "keyring", "fs", *args)

    def mds_asok_all(self, commands):
        """
        admin socket command to all active mds
        """
        from copy import deepcopy

        origin = deepcopy(commands)
        for id in self.fs.mds_ids:
            self.fs.mds_asok(commands, mds_id=id)
            commands = deepcopy(origin)

    def get_subvolume_path(self, subvolume_name):
        return self._fs_cmd("subvolume", "getpath", self.fs.name, subvolume_name).rstrip()

    def enable_qos(self):
        self.mds_asok_all(["config", "set", "mds_dmclock_mds_qos_enable", "true"])

    def is_equal_dict(self, a, b, ignore_key=[]):
        ignore_key = set(ignore_key)
        for key in a:
            if key in ignore_key:
                continue
            if key not in b:
                return False
            if a[key] != b[key]:
                return False
        return True

    def dump_qos(self):
        """
        Get dump qos from all active mds, and then compare the result for each voluem_id.
        if dump qos results for each volume_id are different, assert it.
        """
        result ={}
        ignore = ["session_cnt"]

        for id in self.fs.mds_ids:
            out = self.fs.mds_asok(["dump", "qos"], mds_id=id)  # out == list of volume qos info
            for volume_qos in out:
                self.assertIn("volume_id", volume_qos)

                if volume_qos["volume_id"] in result:
                    self.assertTrue(self.is_equal_dict(volume_qos, result[volume_qos["volume_id"]], ignore_key=ignore))
                result[volume_qos["volume_id"]] = volume_qos

        return list(result.values())

    def test_enable_qos(self):
        """
        Enable QoS for all active mdss.
        """
        self.enable_qos()

        stat_qos = self.dump_qos()

        self.assertNotEqual(stat_qos, [])

    def test_disable_qos(self):
        """
        Disable QoS for all active mdss.
        """
        self.mds_asok_all(["config", "set", "mds_dmclock_mds_qos_enable", "false"])

        stat_qos = self.dump_qos()

        self.assertEqual(stat_qos, [])

    def test_set_default_qos_value(self):
        """
        Enable QoS and set default qos value, and then check the result.
        """
        self.enable_qos()

        reservation, weight, limit = 200, 200, 200

        self.mds_asok_all(["config", "set", "mds_dmclock_mds_qos_default_reservation", str(reservation)])
        self.mds_asok_all(["config", "set", "mds_dmclock_mds_qos_default_limit", str(limit)])
        self.mds_asok_all(["config", "set", "mds_dmclock_mds_qos_default_weight", str(weight)])

        stat_qos = self.dump_qos()

        for info in stat_qos:
            self.assertIn("use_default", info)
            if info["use_default"]:
                self.assertEqual(info["reservation"], reservation)
                self.assertEqual(info["limit"], limit)
                self.assertEqual(info["weight"], weight)

    def test_update_qos_value_root_volume(self):
        """
        Update qos 3 values using setfattr.
        Check its result via getfattr and dump qos.
        """
        self.enable_qos()

        reservation, weight, limit = 100, 100, 100

        self.mount_a.umount_wait()
        self.mount_a.mount(cephfs_mntpt="/")

        self.mount_a.setfattr(self.mount_a.hostfs_mntpt, "ceph.dmclock.mds_reservation", str(reservation))
        self.mount_a.setfattr(self.mount_a.hostfs_mntpt, "ceph.dmclock.mds_limit", str(limit))
        self.mount_a.setfattr(self.mount_a.hostfs_mntpt, "ceph.dmclock.mds_weight", str(weight))

        # check updated vxattr using getfattr
        self.assertEqual(
                int(float(self.mount_a.getfattr(self.mount_a.hostfs_mntpt, "ceph.dmclock.mds_reservation"))),
                reservation)
        self.assertEqual(
                int(float(self.mount_a.getfattr(self.mount_a.hostfs_mntpt, "ceph.dmclock.mds_weight"))),
                weight)
        self.assertEqual(
                int(float(self.mount_a.getfattr(self.mount_a.hostfs_mntpt, "ceph.dmclock.mds_limit"))),
                limit)

        stat_qos = self.dump_qos()
        log.info(stat_qos)

        # check updated dmclock info using dump qos
        for info in stat_qos:
            self.assertIn("volume_id", info)
            if info["volume_id"] == self.get_subvolume_path(self.subvolumes[0]):
                self.assertEqual(info["reservation"], reservation)
                self.assertEqual(info["limit"], limit)
                self.assertEqual(info["weight"], weight)

    def test_update_qos_value(self):
        """
        Update qos 3 values using setfattr.
        Check its result via getfattr and dump qos.
        """
        self.enable_qos()

        reservation, weight, limit = 100, 100, 100

        self.mount_a.setfattr(self.mount_a.hostfs_mntpt, "ceph.dmclock.mds_reservation", str(reservation))
        self.mount_a.setfattr(self.mount_a.hostfs_mntpt, "ceph.dmclock.mds_limit", str(limit))
        self.mount_a.setfattr(self.mount_a.hostfs_mntpt, "ceph.dmclock.mds_weight", str(weight))

        # check updated vxattr using getfattr
        self.assertEqual(
                int(float(self.mount_a.getfattr(self.mount_a.hostfs_mntpt, "ceph.dmclock.mds_reservation"))),
                reservation)
        self.assertEqual(
                int(float(self.mount_a.getfattr(self.mount_a.hostfs_mntpt, "ceph.dmclock.mds_weight"))),
                weight)
        self.assertEqual(
                int(float(self.mount_a.getfattr(self.mount_a.hostfs_mntpt, "ceph.dmclock.mds_limit"))),
                limit)

        stat_qos = self.dump_qos()
        log.info(stat_qos)

        # check updated dmclock info using dump qos
        for info in stat_qos:
            self.assertIn("volume_id", info)
            if info["volume_id"] == self.get_subvolume_path(self.subvolumes[0]):
                self.assertEqual(info["reservation"], reservation)
                self.assertEqual(info["limit"], limit)
                self.assertEqual(info["weight"], weight)

    def test_remote_update_qos_value(self):
        """
        Update qos 3 values using setfattr.
        And check it from another mount path.
        """
        self.enable_qos()

        self.mount_b.umount_wait()
        self.mount_b.mount(cephfs_mntpt=self.get_subvolume_path(self.subvolumes[0]))

        reservation, weight, limit = 50, 50, 50

        self.mount_a.setfattr(self.mount_a.hostfs_mntpt, "ceph.dmclock.mds_reservation", str(reservation))
        self.mount_a.setfattr(self.mount_a.hostfs_mntpt, "ceph.dmclock.mds_limit", str(limit))
        self.mount_a.setfattr(self.mount_a.hostfs_mntpt, "ceph.dmclock.mds_weight", str(weight))

        # check remote vxattr
        self.assertEqual(
                int(float(self.mount_b.getfattr(self.mount_b.hostfs_mntpt, "ceph.dmclock.mds_reservation"))),
                reservation)
        self.assertEqual(
                int(float(self.mount_b.getfattr(self.mount_b.hostfs_mntpt, "ceph.dmclock.mds_weight"))),
                weight)
        self.assertEqual(
                int(float(self.mount_b.getfattr(self.mount_b.hostfs_mntpt, "ceph.dmclock.mds_limit"))),
                limit)

    def test_qos_throttling_50(self):
        import threading

        self.enable_qos()

        reservation, weight, limit = 50, 50, 50

        self.mount_a.setfattr(self.mount_a.hostfs_mntpt, "ceph.dir.pin", str(1))

        self.mount_a.setfattr(self.mount_a.hostfs_mntpt, "ceph.dmclock.mds_reservation", str(reservation))
        self.mount_a.setfattr(self.mount_a.hostfs_mntpt, "ceph.dmclock.mds_limit", str(limit))
        self.mount_a.setfattr(self.mount_a.hostfs_mntpt, "ceph.dmclock.mds_weight", str(weight))

        stat_qos = self.dump_qos()
        log.info(stat_qos)

        threads = []
        results = [0]

        threads.append(threading.Thread(target=create_dirs, args=(0, self.mount_a.hostfs_mntpt, self.TEST_DIR_COUNT, results)))

        log.info("IO Testing...")

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        stat_qos = self.dump_qos()
        log.info(stat_qos)

    def test_qos_ratio(self):
        """
        Test QoS throttling.
        For each subvolume, test QoS using thread.
        """
        import threading

        self.enable_qos()

        reservation, weight, limit = 100, 100, 100

        self.mount_a.setfattr(self.mount_a.hostfs_mntpt, "ceph.dmclock.mds_reservation", str(reservation))
        self.mount_a.setfattr(self.mount_a.hostfs_mntpt, "ceph.dmclock.mds_limit", str(limit))
        self.mount_a.setfattr(self.mount_a.hostfs_mntpt, "ceph.dmclock.mds_weight", str(weight))

        self.mount_b.setfattr(self.mount_b.hostfs_mntpt, "ceph.dmclock.mds_reservation", str(reservation * 2))
        self.mount_b.setfattr(self.mount_b.hostfs_mntpt, "ceph.dmclock.mds_limit", str(limit * 2))
        self.mount_b.setfattr(self.mount_b.hostfs_mntpt, "ceph.dmclock.mds_weight", str(weight * 2))

        stat_qos = self.dump_qos()
        log.info(stat_qos)

        log.info("[Thread 0]: reservation: {0}, weight: {1}, limit {2}".format(reservation, weight, limit))
        log.info("[Thread 1]: reservation: {0}, weight: {1}, limit {2}".format(reservation * 2, weight * 2, limit * 2))

        threads = []
        results = [0, 0]
        threads.append(threading.Thread(target=create_dirs, args=(0, self.mount_a.hostfs_mntpt, self.TEST_DIR_COUNT, results)))
        threads.append(threading.Thread(target=create_dirs, args=(1, self.mount_b.hostfs_mntpt, self.TEST_DIR_COUNT, results)))

        log.info("IO Testing...")

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        stat_qos = self.dump_qos()
        log.info(stat_qos)

        self.assertGreaterEqual(results[0] * 2, results[1] * 0.8)
        self.assertLessEqual(results[0] * 2, results[1] * 1.2)


def create_dirs(tid, base_path, TEST_DIR_COUNT, results):
    from os import path, mkdir
    import time

    base = path.join(base_path, "thread_{0}".format(tid))
    mkdir(base)

    start = time.time()

    for i in range(TEST_DIR_COUNT):
        mkdir(path.join(base, "dir_{0}".format(i)))
        for j in range(TEST_DIR_COUNT):
            mkdir(path.join(base, "dir_{0}".format(i), "subdir_{0}".format(j)))

    elapsed_time = time.time() - start

    results[tid] = (TEST_DIR_COUNT ** 2) / elapsed_time
    log.info("[Thread {0}]: mkdir {1} dirs in {2}secs, OPs/sec: {3}".format(
            tid, TEST_DIR_COUNT ** 2, elapsed_time, (TEST_DIR_COUNT ** 2) / elapsed_time))

