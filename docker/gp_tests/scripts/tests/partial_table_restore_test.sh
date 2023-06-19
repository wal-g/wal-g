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
n=10000
it=10
expected_count=$(($n + $it * 5))

psql -p 6000 -c "DROP DATABASE IF EXISTS db"
psql -p 6000 -c "CREATE DATABASE db"
psql -p 6000 -d db -c "CREATE TABLE heap_to_restore AS SELECT a FROM generate_series(1,$n) AS a;"
psql -p 6000 -d db -c "CREATE TABLE heap_to_skip AS SELECT a FROM generate_series(1,$n) AS a;"
psql -p 6000 -d db -c "CREATE TABLE ao_to_restore(a int, b int) WITH (appendoptimized = true) DISTRIBUTED BY (a);"
psql -p 6000 -d db -c "CREATE TABLE ao_to_skip(a int, b int) WITH (appendoptimized = true) DISTRIBUTED BY (a);"
psql -p 6000 -d db -c "CREATE TABLE co_to_restore(a int, b int) WITH (appendoptimized = true, orientation = column) DISTRIBUTED BY (a);"
psql -p 6000 -d db -c "CREATE TABLE co_to_skip(a int, b int) WITH (appendoptimized = true, orientation = column) DISTRIBUTED BY (a);"
psql -p 6000 -d db -c "INSERT INTO ao_to_restore SELECT i, i FROM generate_series(1,$n)i;"
psql -p 6000 -d db -c "INSERT INTO ao_to_skip SELECT i, i FROM generate_series(1,$n)i;"
psql -p 6000 -d db -c "INSERT INTO co_to_restore SELECT i, i FROM generate_series(1,$n)i;"
psql -p 6000 -d db -c "INSERT INTO co_to_skip SELECT i, i FROM generate_series(1,$n)i;"

# check aovisimap
insert_10_delete_5() {
  start_val=$1
  stop_val=$(($start_val + 9))
  stop_val_d=$(($start_val + 4))
  psql -p 6000 -d db -c "INSERT INTO ao_to_restore SELECT i, i FROM generate_series($start_val,$stop_val)i;"
  psql -p 6000 -d db -c "INSERT INTO ao_to_skip SELECT i, i FROM generate_series($start_val,$stop_val)i;"
  psql -p 6000 -d db -c "INSERT INTO co_to_restore SELECT i, i FROM generate_series($start_val,$stop_val)i;"
  psql -p 6000 -d db -c "INSERT INTO co_to_skip SELECT i, i FROM generate_series($start_val,$stop_val)i;"

  psql -p 6000 -d db -c "DELETE FROM ao_to_restore WHERE a >= $start_val and a <= $stop_val_d;"
  psql -p 6000 -d db -c "DELETE FROM ao_to_skip WHERE a >= $start_val and a <= $stop_val_d;"
  psql -p 6000 -d db -c "DELETE FROM co_to_restore WHERE a >= $start_val and a <= $stop_val_d;"
  psql -p 6000 -d db -c "DELETE FROM co_to_skip WHERE a >= $start_val and a <= $stop_val_d;"
}

for i in $(seq 1 $it);
do
  insert_10_delete_5 $(($n + 1 + 10*($i-1)))
done

run_backup_logged ${TMP_CONFIG} ${PGDATA}
stop_and_delete_cluster_dir

wal-g --config=${TMP_CONFIG} backup-fetch LATEST --in-place --restore-only=db/heap_to_restore,db/ao_to_restore,db/co_to_restore

start_cluster

if [ "$(psql -p 6000 -t -c "SELECT count(*) FROM heap_to_restore;" -d db -A)" != $n ]; then
  echo "Error: Heap table in db database must be restored after partial fetch"
  exit 1
elif [ "$(psql -p 6000 -t -c "SELECT count(*) FROM ao_to_restore;" -d db -A)" != $expected_count ]; then
  echo "Error: Append optimized table in db database must be restored after partial fetch"
  exit 1
elif [ "$(psql -p 6000 -t -c "SELECT count(*) FROM co_to_restore;" -d db -A)" != $expected_count ]; then
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