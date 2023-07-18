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
  psql -p 6000 -c "DROP DATABASE IF EXISTS test"
  psql -p 6000 -c "CREATE DATABASE test"
	psql -p 6000 -d test -c "CREATE TABLE heap AS SELECT a FROM generate_series(1,10) AS a;"
	psql -p 6000 -d test -c "CREATE TABLE ao(a int, b int) WITH (appendoptimized = true) DISTRIBUTED BY (a);"
	psql -p 6000 -d test -c "CREATE TABLE co(a int, b int) WITH (appendoptimized = true, orientation = column) DISTRIBUTED BY (a);"
	psql -p 6000 -d test -c "INSERT INTO ao select i, i FROM generate_series(1,10)i;"
	psql -p 6000 -d test -c "INSERT INTO co select i, i FROM generate_series(1,10)i;"
}

insert_a_lot_of_data() {
  psql -p 6000 -c "DROP DATABASE IF EXISTS test"
  psql -p 6000 -c "CREATE DATABASE test"
	psql -p 6000 -d test -c "CREATE TABLE heap AS SELECT a FROM generate_series(1,100000) AS a;"
	psql -p 6000 -d test -c "CREATE TABLE ao(a int, b int) WITH (appendoptimized = true) DISTRIBUTED BY (a);"
	psql -p 6000 -d test -c "CREATE TABLE co(a int, b int) WITH (appendoptimized = true, orientation = column) DISTRIBUTED BY (a);"
	psql -p 6000 -d test -c "INSERT INTO ao select i, i FROM generate_series(1,100000)i;"
	psql -p 6000 -d test -c "INSERT INTO co select i, i FROM generate_series(1,100000)i;"
}

assert_count() {
  heap=$1
  ao=$2
  co=$3
  
  if [ "$(psql -p 6000 -t -c "SELECT count(*) FROM heap;" -d test -A)" != $heap ]; then
    echo "Error: Heap table in db database must be restored"
    exit 1
  elif [ "$(psql -p 6000 -t -c "SELECT count(*) FROM ao;" -d test -A)" != $ao ]; then
    echo "Error: Heap table in db database must be restored"
    exit 1
  elif [ "$(psql -p 6000 -t -c "SELECT count(*) FROM co;" -d test -A)" != $co ]; then
    echo "Error: Heap table in db database must be restored"
    exit 1
  fi

}

enable_pitr_extension() {
  echo "Enabling gp_pitr extension..."
  psql -p 6000 -d postgres -c "create extension gp_pitr"
}

bootstrap_gp_cluster() {
  source /usr/local/gpdb_src/greenplum_path.sh
  cd /usr/local/gpdb_src
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

stop_cluster() {
  /usr/local/gpdb_src/bin/gpstop -a -M immediate
}

start_cluster() {
  /usr/local/gpdb_src/bin/gpstart -a
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
