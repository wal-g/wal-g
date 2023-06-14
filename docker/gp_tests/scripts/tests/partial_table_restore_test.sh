#!/bin/bash
set -e -x

CONFIG_FILE="/tmp/configs/partial_table_restore_test_config.json"
COMMON_CONFIG="/tmp/configs/common_config.json"
TMP_CONFIG="/tmp/configs/tmp_config.json"
cat ${CONFIG_FILE} > ${TMP_CONFIG}
echo "," >> ${TMP_CONFIG}
cat ${COMMON_CONFIG} >> ${TMP_CONFIG}
/tmp/pg_scripts/wrap_config_file.sh ${TMP_CONFIG}
source /tmp/tests/test_functions/util.sh

wal-g --config=${TMP_CONFIG} delete everything FORCE --confirm

bootstrap_gp_cluster
sleep 3
enable_pitr_extension
setup_wal_archiving

# insert_data
psql -p 6000 -c "DROP DATABASE IF EXISTS db"
psql -p 6000 -c "CREATE DATABASE db"
psql -p 6000 -d db -c "CREATE TABLE heap_to_restore AS SELECT a FROM generate_series(1,10000) AS a;"
psql -p 6000 -d db -c "CREATE TABLE heap_to_skip AS SELECT a FROM generate_series(1,10000) AS a;"
psql -p 6000 -d db -c "CREATE TABLE ao_to_restore(a int, b int) WITH (appendoptimized = true) DISTRIBUTED BY (a);"
psql -p 6000 -d db -c "CREATE TABLE ao_to_skip(a int, b int) WITH (appendoptimized = true) DISTRIBUTED BY (a);"
psql -p 6000 -d db -c "CREATE TABLE co_to_restore(a int, b int) WITH (appendoptimized = true, orientation = column) DISTRIBUTED BY (a);"
psql -p 6000 -d db -c "CREATE TABLE co_to_skip(a int, b int) WITH (appendoptimized = true, orientation = column) DISTRIBUTED BY (a);"
psql -p 6000 -d db -c "INSERT INTO ao_to_restore SELECT i, i FROM generate_series(1,10000)i;"
psql -p 6000 -d db -c "INSERT INTO ao_to_skip SELECT i, i FROM generate_series(1,10000)i;"
psql -p 6000 -d db -c "INSERT INTO co_to_restore SELECT i, i FROM generate_series(1,10000)i;"
psql -p 6000 -d db -c "INSERT INTO co_to_skip SELECT i, i FROM generate_series(1,10000)i;"

run_backup_logged ${TMP_CONFIG} ${PGDATA}
stop_and_delete_cluster_dir

wal-g --config=${TMP_CONFIG} backup-fetch LATEST --in-place --restore-only=db/heap_to_restore,db/ao_to_restore,db/co_to_restore

start_cluster

if [ "$(psql -p 6000 -t -c "SELECT count(*) FROM heap_to_restore;" -d db -A)" != 10000 ]; then
  echo "Error: Heap table in db database must be restored after partial fetch"
  exit 1
elif [ "$(psql -p 6000 -t -c "SELECT count(*) FROM ao_to_restore;" -d db -A)" != 10000 ]; then
  echo "Error: Append optimized table in db database must be restored after partial fetch"
  exit 1
elif [ "$(psql -p 6000 -t -c "SELECT count(*) FROM co_to_restore;" -d db -A)" != 10000 ]; then
  echo "Error: Column oriented table in db database must be restored after partial fetch"
  exit 1
fi

EXPECTED_HEAP_ERROR_MSG="could not open file"
EXPECTED_AO_ERROR_MSG="append-Only storage read could not open segment file"

set +e
heap_output=$(psql -p 6000 -t -c "SELECT count(*) FROM heap_to_skip;" -d db -A 2>&1) 
ao_output=$(psql -p 6000 -t -c "SELECT count(*) FROM ao_to_skip;" -d db -A 2>&1)
aocs_output=$(psql -p 6000 -t -c "SELECT count(*) FROM co_to_skip;" -d db -A 2>&1)
set -e

if ! echo $heap_output | grep -q "$EXPECTED_HEAP_ERROR_MSG"; then
  echo "Error: to_skip database directory must be emtpy after partial fetch"
  echo $heap_output
  exit 1
elif ! echo $ao_output | grep -q "$EXPECTED_AO_ERROR_MSG"; then
  echo "Error: to_skip database directory must be emtpy after partial fetch"
  echo $ao_output
  exit 1
elif ! echo $aocs_output | grep -q "$EXPECTED_AO_ERROR_MSG"; then
  echo "Error: to_skip database directory must be emtpy after partial fetch"
  echo $aocs_output
  exit 1
fi

cleanup