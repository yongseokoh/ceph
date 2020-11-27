#!/bin/bash

set -x

function run_test() {

    VSTART_LOG_PATH="vstart_log"
    mkdir -p $VSTART_LOG_PATH
    VSTART_LOG_PATH="$VSTART_LOG_PATH/$(date +experiment-%Y%m%d-%H:%M)-${3}"
    mkdir -p $VSTART_LOG_PATH

    for testfile in `cat ${1}`
    do
        filename=$(basename $testfile)
        filename="${filename%%.*}"

        echo test name $filename

        #echo "Wait for debugging, please enter any keys"
        #read test

        ./start_qos_enabled.sh $2

        python ../qa/tasks/vstart_runner.py tasks.cephfs.${filename} --run-all-tests

        mv vstart_runner.log ${VSTART_LOG_PATH}/${filename}.log

        sleep 5
        ./stop.sh

        sleep 5
        ./kill-fuse.sh

        sleep 5
        ./umount_fuse_all.sh
    done
}

TEST_FILE=vstart_passed_test.txt

run_test $TEST_FILE true qos-on
run_test $TEST_FILE false qos-off
