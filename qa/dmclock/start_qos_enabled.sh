#!/bin/bash

NUM_MDS=2
VSTART_DEST=/ceph/build MON=1 OSD=1 MDS=${NUM_MDS} ../src/vstart.sh -l -n -d -x --smallmds --without-dashboard --multimds ${NUM_MDS} 

BUILD_PATH=/ceph/build
R=5000
W=5000
L=5000

${BUILD_PATH}/bin/ceph fs set a max_mds $NUM_MDS

${BUILD_PATH}/bin/ceph --admin-daemon ${BUILD_PATH}/out/mds.a.asok config set mds_dmclock_enable $1
${BUILD_PATH}/bin/ceph --admin-daemon ${BUILD_PATH}/out/mds.b.asok config set mds_dmclock_enable $1

${BUILD_PATH}/bin/ceph --admin-daemon ${BUILD_PATH}/out/mds.a.asok config set mds_dmclock_reservation ${R}
${BUILD_PATH}/bin/ceph --admin-daemon ${BUILD_PATH}/out/mds.a.asok config set mds_dmclock_weight ${W}
${BUILD_PATH}/bin/ceph --admin-daemon ${BUILD_PATH}/out/mds.a.asok config set mds_dmclock_limit ${L}

${BUILD_PATH}/bin/ceph --admin-daemon ${BUILD_PATH}/out/mds.b.asok config set mds_dmclock_reservation ${R}
${BUILD_PATH}/bin/ceph --admin-daemon ${BUILD_PATH}/out/mds.b.asok config set mds_dmclock_weight ${W}
${BUILD_PATH}/bin/ceph --admin-daemon ${BUILD_PATH}/out/mds.b.asok config set mds_dmclock_limit ${L}

${BUILD_PATH}/bin/ceph --admin-daemon out/mds.a.asok config set debug_mgrc 1/5
${BUILD_PATH}/bin/ceph --admin-daemon out/mds.a.asok config set debug_monc 1/5

${BUILD_PATH}/bin/ceph --admin-daemon out/mds.b.asok config set debug_mgrc 1/5
${BUILD_PATH}/bin/ceph --admin-daemon out/mds.b.asok config set debug_monc 1/5

${BUILD_PATH}/bin/ceph --admin-daemon out/mds.a.asok config set debug_ms 0/0
${BUILD_PATH}/bin/ceph --admin-daemon out/mds.b.asok config set debug_ms 0/0

${BUILD_PATH}/bin/ceph --admin-daemon out/mds.a.asok config set debug_mds 1/5
${BUILD_PATH}/bin/ceph --admin-daemon out/mds.b.asok config set debug_mds 1/5
