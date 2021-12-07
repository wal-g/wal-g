#!/bin/bash
set -e -x

CONFIG_FILE="/tmp/configs/full_backup_test_config.json"
COMMON_CONFIG="/tmp/configs/common_config.json"
TMP_CONFIG="/tmp/configs/tmp_config.json"
cat ${CONFIG_FILE} > ${TMP_CONFIG}
echo "," >> ${TMP_CONFIG}
cat ${COMMON_CONFIG} >> ${TMP_CONFIG}
/tmp/pg_scripts/wrap_config_file.sh ${TMP_CONFIG}
source /tmp/tests/test_functions/util.sh

bootstrap_gp_cluster
sleep 3
enable_pitr_extension
setup_wal_archiving

wal-g backup-push --config=${TMP_CONFIG}
stop_and_delete_cluster_dir

# show the backup list
wal-g backup-list --config=${TMP_CONFIG}

# show the storage objects (useful for debug)
wal-g st ls -r --config=${TMP_CONFIG}

wal-g backup-fetch LATEST --in-place --config=${TMP_CONFIG}

start_cluster
cleanup

echo "Greenplum backup-push test was successful"