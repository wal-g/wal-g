#!/bin/bash
set -e -x
CONFIG_FILE="/tmp/configs/ao_storage_test_config.json"

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

# 1st backup (init tables heap, ao, co)
insert_data
WALG_GP_AOSEG_SIZE_THRESHOLD=1048576 wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}

wal-g st ls -r --config=${TMP_CONFIG}
# AO/AOCS storage should be empty (all objects are below the threshold)
if (wal-g st ls -r --config=${TMP_CONFIG} | grep -q "_aoseg") then
  echo "Error: AO/AOCS shared storage is not empty"
  exit 1
fi

# 2nd backup (populate the co table)
psql -p 6000 -d test -c "INSERT INTO co select i, i FROM generate_series(1,10)i;"
run_backup_logged ${TMP_CONFIG} ${PGDATA}

# 3rd backup (populate the ao table)
psql -p 6000 -d test -c "INSERT INTO ao select i, i FROM generate_series(1,10)i;"
run_backup_logged ${TMP_CONFIG} ${PGDATA}

backup_name=$(wal-g --config=${TMP_CONFIG} backup-list | tail -n 1 | cut -f 1 -d " ")

wal-g --config=${TMP_CONFIG} backup-list

wal-g --config=${TMP_CONFIG} delete before $backup_name --confirm

# show the storage objects (useful for debug)
wal-g st ls -r --config=${TMP_CONFIG}

stop_and_delete_cluster_dir

wal-g backup-fetch LATEST --in-place --config=${TMP_CONFIG}
start_cluster

psql -p 6000 -d test -c "SELECT COUNT(*) FROM ao;" | grep 20 && EXIT_STATUS=$? || EXIT_STATUS=$?
if [ "$EXIT_STATUS" -ne 0 ] ; then
    echo "Error: Failed to read from ao table after restore"
    exit 1
fi

psql -p 6000 -d test -c "SELECT COUNT(*) FROM co;" | grep 20 && EXIT_STATUS=$? || EXIT_STATUS=$?
if [ "$EXIT_STATUS" -ne 0 ] ; then
    echo "Error: Failed to read from co table after restore"
    exit 1
fi

cleanup
rm ${TMP_CONFIG}
