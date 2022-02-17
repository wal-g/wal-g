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
  psql -p 6000 -c "drop database if exists test"
  psql -p 6000 -c "create database test"
  psql -p 6000 -d test -c "create table test(id serial not null primary key, n float)"
  psql -p 6000 -d test -c "insert into test(n) select random() from generate_series(1,10000)"
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