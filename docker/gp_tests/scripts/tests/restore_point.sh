#!/bin/bash
set -e -x
CONFIG_FILE="/tmp/configs/create_restore_point_config.json"

COMMON_CONFIG="/tmp/configs/common_config.json"
TMP_CONFIG="/tmp/configs/tmp_config.json"
cat ${CONFIG_FILE} > ${TMP_CONFIG}
echo "," >> ${TMP_CONFIG}
cat ${COMMON_CONFIG} >> ${TMP_CONFIG}
/tmp/pg_scripts/wrap_config_file.sh ${TMP_CONFIG}
source /tmp/tests/test_functions/util.sh

bootstrap_gp_cluster
sleep 3
setup_wal_archiving
enable_pitr_extension

wal-g --config=${TMP_CONFIG} delete everything FORCE --confirm

wal-g create-restore-point rp1 --config=${TMP_CONFIG}
wal-g create-restore-point rp2 --config=${TMP_CONFIG}

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

# should not fail
wal-g backup-fetch LATEST --restore-point after_backup --in-place --config=${TMP_CONFIG}
delete_cluster_dirs

# should not fail
wal-g backup-fetch --restore-point after_backup --in-place --config=${TMP_CONFIG}

start_cluster

cleanup
rm ${TMP_CONFIG}
