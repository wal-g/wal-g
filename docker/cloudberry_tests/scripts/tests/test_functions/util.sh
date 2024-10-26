#!/bin/bash
set -e

declare -a SEGMENTS_DIRS=(
  '-1 /usr/local/gpdb_src/gpAux/gpdemo/datadirs/qddir/demoDataDir-1'
  '0 /usr/local/gpdb_src/gpAux/gpdemo/datadirs/dbfast1/demoDataDir0'
  '1 /usr/local/gpdb_src/gpAux/gpdemo/datadirs/dbfast2/demoDataDir1'
  '2 /usr/local/gpdb_src/gpAux/gpdemo/datadirs/dbfast3/demoDataDir2'
)

insert_data() {
  echo "Inserting sample data..."
  psql -p 7000 -c "DROP DATABASE IF EXISTS test"
  psql -p 7000 -c "CREATE DATABASE test"
	psql -p 7000 -d test -c "CREATE TABLE heap AS SELECT a FROM generate_series(1,10) AS a;"
	psql -p 7000 -d test -c "CREATE TABLE ao(a int, b int) WITH (appendoptimized = true) DISTRIBUTED BY (a);"
	psql -p 7000 -d test -c "CREATE TABLE co(a int, b int) WITH (appendoptimized = true, orientation = column) DISTRIBUTED BY (a);"
	psql -p 7000 -d test -c "INSERT INTO ao select i, i FROM generate_series(1,10)i;"
	psql -p 7000 -d test -c "INSERT INTO co select i, i FROM generate_series(1,10)i;"
}

insert_a_lot_of_data() {
  psql -p 7000 -c "DROP DATABASE IF EXISTS test"
  psql -p 7000 -c "CREATE DATABASE test"
	psql -p 7000 -d test -c "CREATE TABLE heap AS SELECT a FROM generate_series(1,100000) AS a;"
	psql -p 7000 -d test -c "CREATE TABLE ao(a int, b int) WITH (appendoptimized = true) DISTRIBUTED BY (a);"
	psql -p 7000 -d test -c "CREATE TABLE co(a int, b int) WITH (appendoptimized = true, orientation = column) DISTRIBUTED BY (a);"
	psql -p 7000 -d test -c "INSERT INTO ao select i, i FROM generate_series(1,100000)i;"
	psql -p 7000 -d test -c "INSERT INTO co select i, i FROM generate_series(1,100000)i;"
}

bootstrap_gp_cluster() {
  source /usr/local/gpdb_src/greenplum_path.sh
  cd /usr/local/gpdb_src
  # FIXME: mirrors & standby?
  export WITH_STANDBY="false"
  export WITH_MIRRORS="false"
  make create-demo-cluster

  source /usr/local/gpdb_src/gpAux/gpdemo/gpdemo-env.sh && /usr/local/gpdb_src/bin/createdb
}

cleanup() {
  source /usr/local/gpdb_src/greenplum_path.sh
  cd /usr/local/gpdb_src
  make destroy-demo-cluster
  pkill -9 postgres || true
  pkill -9 wal-g || true
}

die_with_cb_logs() {
    for elem in "${SEGMENTS_DIRS[@]}"; do
      read -a arr <<< "$elem"
      echo "*** ${arr[1]} ***"
      more "${arr[1]}"/log/* | cat || true
    done
    exit 1
}

stop_cluster() {
  /usr/local/gpdb_src/bin/gpstop -a -M fast
}

prepare_cluster() {
  # start Coordinator only... it will fail
  /usr/local/gpdb_src/bin/gpstart -c -a -t 180 || true
  # then wait until coordinator recovery finished & it will start accepting connections:
  for i in {1..180}; do PGOPTIONS='-c gp_role=utility' psql -p 7000 -d postgres -c " select 1;" && break || sleep 1; done
  # here we can do all we need to fix cluster configuration
  # e.g. update gp_segment_configuration... however we don't need to do it in this tests
  # stop cluster...
  /usr/local/gpdb_src/bin/gpstop -c -a -M fast || true

  # This start will work as expected:
  /usr/local/gpdb_src/bin/gpstart -a -t 180 || true
  # repair if anything is broken:
  /usr/local/gpdb_src/bin/gprecoverseg -F -a || true
  # cleanup:
  /usr/local/gpdb_src/bin/gpstop -a -M fast || true
  # always remove recovery.conf: so, Cloudberry can be safely restarted
  for elem in "${SEGMENTS_DIRS[@]}"; do
    read -a arr <<< "$elem"
    rm "${arr[1]}"/recovery.signal || true
    echo "" > "${arr[1]}"/conf.d/recovery.conf || true
  done
}

start_cluster() {
  /usr/local/gpdb_src/bin/gpstart -a -t 180 || die_with_cb_logs
}

setup_wal_archiving() {
  for elem in "${SEGMENTS_DIRS[@]}"; do
    read -a arr <<< "$elem"
      echo "
  wal_level = archive
  archive_mode = on
  archive_timeout = 600
  archive_command = '/usr/bin/timeout 60 wal-g seg wal-push %p --content-id=${arr[0]} --config ${TMP_CONFIG}'
  " >> ${arr[1]}/postgresql.conf
  done
  stop_cluster
  start_cluster
}

delete_cluster_dirs() {
  #remove the database files
  for elem in "${SEGMENTS_DIRS[@]}"; do
    read -a arr <<< "$elem"
    rm -rf "${arr[1]}"
  done
}

stop_and_delete_cluster_dir() {
  stop_cluster
  delete_cluster_dirs
}

run_backup_logged() {
  wal-g --config=$1 backup-push $2 $3 && EXIT_STATUS=$? || EXIT_STATUS=$?
  if [ "$EXIT_STATUS" -ne 0 ] ; then
      echo "Error: Failed to create backup"
      cat /var/log/wal-g-gplog.log
      cat /var/log/wal-g-log-seg*
      exit 1
  fi
}
