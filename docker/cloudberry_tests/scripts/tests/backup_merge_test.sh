#!/bin/bash
set -e -x

CONFIG_FILE="/tmp/configs/backup_merge_test_config.json"
COMMON_CONFIG="/tmp/configs/common_config.json"
TMP_CONFIG="/tmp/configs/tmp_backup_merge_config.json"

cat ${CONFIG_FILE} > ${TMP_CONFIG}
echo "," >> ${TMP_CONFIG}
cat ${COMMON_CONFIG} >> ${TMP_CONFIG}
/tmp/pg_scripts/wrap_config_file.sh ${TMP_CONFIG}

source /tmp/tests/test_functions/util.sh

bootstrap_gp_cluster
setup_wal_archiving

wal-g --config=${TMP_CONFIG} delete everything FORCE --confirm

echo "Creating initial data..."
echo "Inserting sample data..."
psql -p 7000 -c "DROP DATABASE IF EXISTS test"
psql -p 7000 -c "CREATE DATABASE test"
psql -p 7000 -d test -c "CREATE TABLE heap AS SELECT a FROM generate_series(1,10) AS a;"
psql -p 7000 -d test -c "INSERT INTO heap (a) SELECT a FROM generate_series(1, 10) AS a;"
# Create minimal AO tables (row and column oriented) and insert initial data
psql -p 7000 -d test -c "CREATE TABLE ao_row (a int) WITH (appendonly=true) DISTRIBUTED BY (a);"
psql -p 7000 -d test -c "INSERT INTO ao_row SELECT a FROM generate_series(1,10) AS a(a);"
psql -p 7000 -d test -c "CREATE TABLE ao_col (a int, b int) WITH (appendonly=true, orientation=column) DISTRIBUTED BY (a);"
psql -p 7000 -d test -c "INSERT INTO ao_col SELECT a, a*2 FROM generate_series(1,10) AS a(a);"

echo "Creating base backup..."
run_backup_logged ${TMP_CONFIG} ${PGDATA} "--full"
BASE_BACKUP_NAME=$(wal-g --config=${TMP_CONFIG} backup-list | tail -n 1 | cut -f 1 -d ' ')
echo "Base backup created: ${BASE_BACKUP_NAME}"

echo "Adding more data for first delta backup..."
psql -p 7000 -d test -c "INSERT INTO heap (a) SELECT a FROM generate_series(11,20) AS a;"
# AO tables delta-1 inserts
psql -p 7000 -d test -c "INSERT INTO ao_row SELECT a FROM generate_series(11,20) AS a(a);"
psql -p 7000 -d test -c "INSERT INTO ao_col SELECT a, a*2 FROM generate_series(11,20) AS a(a);"

echo "Creating first delta backup..."
run_backup_logged ${TMP_CONFIG} ${PGDATA}
FIRST_DELTA_BACKUP_NAME=$(wal-g --config=${TMP_CONFIG} backup-list | tail -n 1 | cut -f 1 -d ' ')
echo "First delta backup created: ${FIRST_DELTA_BACKUP_NAME}"

echo "Adding more data for second delta backup..."
psql -p 7000 -d test -c "INSERT INTO heap (a) SELECT a FROM generate_series(21,30) AS a;"
# AO tables delta-2 inserts
psql -p 7000 -d test -c "INSERT INTO ao_row SELECT a FROM generate_series(21,30) AS a(a);"
psql -p 7000 -d test -c "INSERT INTO ao_col SELECT a, a*2 FROM generate_series(21,30) AS a(a);"

echo "Creating second delta backup..."
run_backup_logged ${TMP_CONFIG} ${PGDATA}
SECOND_DELTA_BACKUP_NAME=$(wal-g --config=${TMP_CONFIG} backup-list | tail -n 1 | cut -f 1 -d ' ')
echo "Second delta backup created: ${SECOND_DELTA_BACKUP_NAME}"

echo "Creating data dump before merge..."
pg_dumpall -p 7000 -f /tmp/dump_before_merge

echo "Testing backup-merge command..."
wal-g --config=${TMP_CONFIG} backup-merge ${SECOND_DELTA_BACKUP_NAME}

MERGED_BACKUP_NAME=$(echo "${SECOND_DELTA_BACKUP_NAME}" | sed 's/_D_[^_]*//g')
echo "Expected merged backup name: ${MERGED_BACKUP_NAME}"

echo "Checking backup list after merge..."
wal-g --config=${TMP_CONFIG} backup-list

if wal-g --config=${TMP_CONFIG} backup-list | grep -q "${MERGED_BACKUP_NAME}"; then
    echo "Merged backup found in backup list: ${MERGED_BACKUP_NAME}"
else
    echo "Error: Merged backup not found in backup list"
    exit 1
fi

echo "Stopping cluster after merge..."
stop_cluster
delete_cluster_dirs

echo "Testing restore from merged backup..."
wal-g --config=${TMP_CONFIG} backup-fetch ${MERGED_BACKUP_NAME} --in-place

echo "Preparing and starting cluster after restore..."
prepare_cluster
start_cluster

echo "Creating data dump after restore..."
pg_dumpall -p 7000 -f /tmp/dump_after_merge

echo "Comparing dumps..."
if diff /tmp/dump_before_merge /tmp/dump_after_merge; then
    echo "SUCCESS: Dumps are identical - backup-merge works correctly"
else
    echo "ERROR: Dumps differ - backup-merge failed"
    exit 1
fi

echo "Verifying data integrity after restore..."
psql -p 7000 -d test -c "SELECT COUNT(*) FROM heap;" | grep 40 && EXIT_STATUS=$? || EXIT_STATUS=$?
if [ "$EXIT_STATUS" -ne 0 ] ; then
    echo "Error: Failed to read correct count from heap table after restore (expected 40)"
    exit 1
fi
# Verify AO row-oriented table count (use unaligned tuples-only output for robust grep)
AO_ROW_COUNT=$(psql -p 7000 -d test -t -A -c "SELECT COUNT(*) FROM ao_row;")
if [ "$AO_ROW_COUNT" != "30" ] ; then
    echo "Error: Failed to read correct count from AO row table after restore (expected 30, got $AO_ROW_COUNT)"
    exit 1
fi
# Verify AO column-oriented table count
AO_COL_COUNT=$(psql -p 7000 -d test -t -A -c "SELECT COUNT(*) FROM ao_col;")
if [ "$AO_COL_COUNT" != "30" ] ; then
    echo "Error: Failed to read correct count from AO column table after restore (expected 30, got $AO_COL_COUNT)"
    exit 1
fi

cleanup
rm ${TMP_CONFIG}

echo "Cloudberry backup merge test completed successfully"