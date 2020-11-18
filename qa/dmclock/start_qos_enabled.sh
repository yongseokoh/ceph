#!/bin/bash

NUM_MDS=2
VSTART_DEST=~/ceph/build MON=1 OSD=1 MDS=${NUM_MDS} ../src/vstart.sh -l -n -d -x --smallmds --without-dashboard --multimds ${NUM_MDS} 

BUILD_PATH=~/ceph/build
R=100
W=100
L=100

${BUILD_PATH}/bin/ceph fs set a max_mds $NUM_MDS

${BUILD_PATH}/bin/ceph --admin-daemon ${BUILD_PATH}/out/mds.a.asok config set mds_dmclock_mds_qos_enable $1
${BUILD_PATH}/bin/ceph --admin-daemon ${BUILD_PATH}/out/mds.b.asok config set mds_dmclock_mds_qos_enable $1

${BUILD_PATH}/bin/ceph --admin-daemon ${BUILD_PATH}/out/mds.a.asok config set mds_dmclock_mds_qos_default_reservation ${R}
${BUILD_PATH}/bin/ceph --admin-daemon ${BUILD_PATH}/out/mds.a.asok config set mds_dmclock_mds_qos_default_weight ${W}
${BUILD_PATH}/bin/ceph --admin-daemon ${BUILD_PATH}/out/mds.a.asok config set mds_dmclock_mds_qos_default_limit ${L}

${BUILD_PATH}/bin/ceph --admin-daemon ${BUILD_PATH}/out/mds.b.asok config set mds_dmclock_mds_qos_default_reservation ${R}
${BUILD_PATH}/bin/ceph --admin-daemon ${BUILD_PATH}/out/mds.b.asok config set mds_dmclock_mds_qos_default_weight ${W}
${BUILD_PATH}/bin/ceph --admin-daemon ${BUILD_PATH}/out/mds.b.asok config set mds_dmclock_mds_qos_default_limit ${L}
