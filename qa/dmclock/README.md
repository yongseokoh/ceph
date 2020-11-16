
export CEPH_BUILD_PATH=~/ceph/build
$cp start_all_test.sh start_qos_enabled.sh vstart_all_test.txt vstart_passed_test.txt $CEPH_BUILD_PATH
$cd $CEPH_BUILD_PATH
$./start_all_test.sh vstart_passed_test.txt
