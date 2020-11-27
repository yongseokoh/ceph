#!/bin/bash

set -x

MOUNT_PATH="/mnt/smallfile"

function run_smallfile_test() {

    SMALLFILE_LOG_PATH="smallfile_log"
    mkdir -p $SMALLFILE_LOG_PATH
    SMALLFILE_LOG_PATH="$SMALLFILE_LOG_PATH/$(date +experiment-%Y%m%d-%H:%M)-${2}"
    mkdir -p $SMALLFILE_LOG_PATH

    ./start_qos_enabled.sh $1

    mkdir -p $MOUNT_PATH

    sudo bin/ceph-fuse -n client.admin -c ceph.conf -k keyring --client-mountpoint=/ $MOUNT_PATH

    ops="create stat chmod symlink ls-l rename cleanup"

    for op in $ops
    do
        python smallfile/smallfile_cli.py --operation $op --thread 4 --file-size 0 --files-per-dir 10000  --files 10000 --top $MOUNT_PATH --output-json $SMALLFILE_LOG_PATH/${op}.json
    done

    sleep 5
    ./stop.sh

    sleep 5
    ./kill-fuse.sh

    sleep 5
    ./umount_fuse_all.sh
}

if [ ! -d "smallfile" ] ; then
    git clone https://github.com/distributed-system-analysis/smallfile
fi

run_smallfile_test true qos-on
#run_smallfile_test false qos-off

if [[ "$1" == "poweroff" ]]; then
    sudo shutdown -h now
fi
