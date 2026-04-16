#!/bin/bash
set -e -x
CONFIG_FILE="/tmp/configs/create_restore_point_config.json"

COMMON_CONFIG="/tmp/configs/common_config.json"
TMP_CONFIG="/tmp/configs/tmp_config.json"
jq -s '.[0] * .[1]' ${COMMON_CONFIG} ${CONFIG_FILE} > ${TMP_CONFIG}
source /tmp/tests/test_functions/util.sh

bootstrap_gp_cluster
setup_wal_archiving

wal-g --config=${TMP_CONFIG} delete everything FORCE --confirm

wal-g create-restore-point rp1 --config=${TMP_CONFIG}
wal-g create-restore-point rp2 --config=${TMP_CONFIG}

# Check whether the WAL log is correctly switched and uploaded to S3 after create-restore-point
# gpadmin@10f4a227f02b:/usr/local/gpdb_src$ wal-g st ls  segments_005/seg0/wal_005/ --config=${TMP_CONFIG}
# type size    last modified                     name
# obj  4624920 2025-05-21 07:17:16.052 +0000 UTC 000000010000000000000001.lz4
# obj  264275  2025-05-21 07:26:06.265 +0000 UTC 000000010000000000000002.lz4

#wait for wal-g to upload WALs
sleep 5

check_wal_upload() {
    local path=$1

    wal-g st ls "$path" --config=${TMP_CONFIG}

    wal-g st ls "$path" --config=${TMP_CONFIG} \
        | awk '/^obj/ {count++} END {exit !(count >= 2)}'
}

# Check each segment
for seg_path in \
    segments_005/seg-1/wal_005/ \
    segments_005/seg0/wal_005/ \
    segments_005/seg1/wal_005/ \
    segments_005/seg2/wal_005/
do
    if ! check_wal_upload "$seg_path"; then
        echo "Error: WAL files after create-restore-point were not correctly uploaded to S3 for $seg_path"
        exit 1
    fi
done


# check verify results to end with 'OK'
if ! (wal-g st ls basebackups_005 --config=${TMP_CONFIG} | grep -q 'rp1_restore_point.json') then
  echo "Error: restore point rp1 metadata file does not exist"
  exit 1
fi

if ! (wal-g st ls basebackups_005 --config=${TMP_CONFIG} | grep -q 'rp2_restore_point.json') then
  echo "Error: restore point rp2 metadata file does not exist"
  exit 1
fi

wal-g create-restore-point rp1 --config=${TMP_CONFIG} && EXIT_STATUS=$? || EXIT_STATUS=$?

if [ "$EXIT_STATUS" -eq 0 ] ; then
    echo "Error: Duplicate restore point has been created"
    exit 1
fi

wal-g backup-push --config=${TMP_CONFIG}
TIME_AFTER_BACKUP=$(date +'%Y-%m-%dT%H:%M:%S.%NZ')

wal-g create-restore-point after_backup --config=${TMP_CONFIG}
stop_and_delete_cluster_dir

wal-g backup-fetch LATEST --restore-point rp1 --in-place --config=${TMP_CONFIG} && EXIT_STATUS=$? || EXIT_STATUS=$?
if [ "$EXIT_STATUS" -eq 0 ] ; then
    echo "Error: backup fetched with restore point in the past"
    exit 1
fi

wal-g backup-fetch --restore-point rp1 --in-place --config=${TMP_CONFIG} && EXIT_STATUS=$? || EXIT_STATUS=$?
if [ "$EXIT_STATUS" -eq 0 ] ; then
    echo "Error: backup fetched with restore point in the past"
    exit 1
fi

wal-g restore-point-list --config=${TMP_CONFIG}
# should pick backup restore point
wal-g backup-fetch LATEST --restore-point-ts=${TIME_AFTER_BACKUP} --in-place --config=${TMP_CONFIG}
delete_cluster_dirs

# should not fail
wal-g backup-fetch LATEST --restore-point after_backup --in-place --config=${TMP_CONFIG}
delete_cluster_dirs

# should not fail
wal-g backup-fetch --restore-point after_backup --in-place --config=${TMP_CONFIG}
prepare_cluster
start_cluster

cleanup
rm ${TMP_CONFIG}
