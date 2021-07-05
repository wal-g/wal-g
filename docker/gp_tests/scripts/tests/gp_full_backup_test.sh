#!/bin/bash
set -e -x

export WALG_FILE_PREFIX='/tmp/wal-g-test-data'
export WALG_LOG_LEVEL=DEVEL

/home/gpadmin/run_greenplum.sh

source /usr/local/gpdb_src/gpAux/gpdemo/gpdemo-env.sh && /usr/local/gpdb_src/bin/createdb
sleep 10

mkdir $WALG_FILE_PREFIX

wal-g-gp backup-push

echo "Greenplum backup-push test was successful"