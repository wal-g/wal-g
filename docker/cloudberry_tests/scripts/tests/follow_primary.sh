#!/bin/bash
set -e -x
CONFIG_FILE="/tmp/configs/create_restore_point_config.json"
RESTORE_CONFIG="/tmp/configs/follow_primary_restore_config.json"
COMMON_CONFIG="/tmp/configs/common_config.json"
TMP_CONFIG="/tmp/configs/tmp_config.json"
cat ${CONFIG_FILE} > ${TMP_CONFIG}
echo "," >> ${TMP_CONFIG}
cat ${COMMON_CONFIG} >> ${TMP_CONFIG}
/tmp/pg_scripts/wrap_config_file.sh ${TMP_CONFIG}
source /tmp/tests/test_functions/util.sh

bootstrap_gp_cluster
setup_wal_archiving

wal-g --config=${TMP_CONFIG} delete everything FORCE --confirm

wal-g backup-push --config=${TMP_CONFIG}
echo "Inserting sample data..."
psql -p 7000 -c "DROP DATABASE IF EXISTS test"
psql -p 7000 -c "CREATE DATABASE test"
psql -p 7000 -d test -c "CREATE TABLE heap AS SELECT a FROM generate_series(1,10) AS a;"
wal-g create-restore-point rp1 --config=${TMP_CONFIG}
psql -p 7000 -d test -c "INSERT INTO heap (a) SELECT a FROM generate_series(1, 10) AS a;"
wal-g create-restore-point rp2 --config=${TMP_CONFIG}
psql -p 7000 -d test -c "INSERT INTO heap (a) SELECT a FROM generate_series(1, 10) AS a;"
wal-g create-restore-point rp3 --config=${TMP_CONFIG}
wal-g backup-list --config=${TMP_CONFIG}
wal-g restore-point-list --config=${TMP_CONFIG}
stop_and_delete_cluster_dir

wal-g backup-fetch LATEST --restore-config=${RESTORE_CONFIG} --config=${TMP_CONFIG}
wal-g recovery-action shutdown --restore-config=${RESTORE_CONFIG} --config=${TMP_CONFIG}
wal-g follow-primary rp1 --restore-config=${RESTORE_CONFIG} --config=${TMP_CONFIG}
sleep 30
wal-g follow-primary rp2 --restore-config=${RESTORE_CONFIG} --config=${TMP_CONFIG}
sleep 30
wal-g follow-primary LATEST --restore-config=${RESTORE_CONFIG} --config=${TMP_CONFIG}
sleep 30
wal-g recovery-action promote --restore-config=${RESTORE_CONFIG} --config=${TMP_CONFIG}
prepare_cluster
start_cluster
test "$(psql -p 7000 -d test -t -A -c "SELECT count(*) FROM heap;")" -eq 30 || { echo "Test failed: The count is not equal to 30."; exit 1; }

cleanup
rm ${TMP_CONFIG}
