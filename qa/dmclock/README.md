
export CEPH_BUILD_PATH=~/ceph/build
./install.sh $CEPH_BUILD_PATH
cd $CEPH_BUILD_PATH
./start_all_test.sh vstart_passed_test.txt
./start_smallfile.sh
