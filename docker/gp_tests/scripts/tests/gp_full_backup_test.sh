#!/bin/bash
set -e -x

export WALG_FILE_PREFIX='/tmp/wal-g-test-data'

/home/gpadmin/run_greenplum.sh

source /usr/local/gpdb_src/gpAux/gpdemo/gpdemo-env.sh && /usr/local/gpdb_src/bin/createdb
sleep 10

mkdir $WALG_FILE_PREFIX

wal-g-gp backup-push /usr/local/gpdb_src/gpAux/gpdemo/datadirs/qddir/demoDataDir-1

echo "Greenplum backup-push test was successful"