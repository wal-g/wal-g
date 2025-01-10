#!/bin/bash
set -e -x

FAILOVER_STORAGE_CONFIG_FILE="/tmp/configs/full_backup_test_failover_storage_config.json"
FAILOVER_STORAGE_TMP_CONFIG="/tmp/configs/tmp_config_failover_storage.json"

CONFIG_FILE="/tmp/configs/full_backup_test_config.json"
COMMON_CONFIG="/tmp/configs/common_config.json"
TMP_CONFIG="/tmp/configs/tmp_config.json"
cat ${CONFIG_FILE} > ${TMP_CONFIG}
echo "," >> ${TMP_CONFIG}
cat ${COMMON_CONFIG} >> ${TMP_CONFIG}
/tmp/pg_scripts/wrap_config_file.sh ${TMP_CONFIG}

cat ${FAILOVER_STORAGE_CONFIG_FILE} > ${FAILOVER_STORAGE_TMP_CONFIG}
echo "," >> ${FAILOVER_STORAGE_TMP_CONFIG}
cat ${COMMON_CONFIG} >> ${FAILOVER_STORAGE_TMP_CONFIG}
/tmp/pg_scripts/wrap_config_file.sh ${FAILOVER_STORAGE_TMP_CONFIG}

source /tmp/tests/test_functions/util.sh

bootstrap_gp_cluster
enable_pitr_extension
setup_wal_archiving

# 1st backup (init tables heap, ao, co)
insert_data
run_backup_logged ${FAILOVER_STORAGE_TMP_CONFIG} ${PGDATA}

# 2nd backup (populate the co table)
psql -p 6000 -d test -c "INSERT INTO co select i, i FROM generate_series(1,10)i;"
run_backup_logged ${FAILOVER_STORAGE_TMP_CONFIG} ${PGDATA}

# 3rd backup (populate the ao table)
psql -p 6000 -d test -c "INSERT INTO ao select i, i FROM generate_series(1,10)i;"
run_backup_logged ${FAILOVER_STORAGE_TMP_CONFIG} ${PGDATA}

stop_and_delete_cluster_dir

# show the backup list
wal-g backup-list --config=${FAILOVER_STORAGE_TMP_CONFIG}

# show the storage objects (useful for debug)
wal-g st ls -r --config=${TMP_CONFIG}

wal-g backup-fetch LATEST --in-place --config=${FAILOVER_STORAGE_TMP_CONFIG}

start_cluster
cleanup

echo "Greenplum backup-push test was successful"