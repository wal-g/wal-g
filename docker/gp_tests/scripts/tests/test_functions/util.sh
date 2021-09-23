#!/bin/bash
set -e

insert_data() {
  echo "Inserting sample data..."
  psql -p 7000 -c "drop database if exists test"
  psql -p 7000 -c "create database test"
  psql -p 7000 -d test -c "create table test(id integer primary key generated always as identity, n float)"
  psql -p 7000 -d test -c "insert into test(n) select random() from generate_series(1,10000)"
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
