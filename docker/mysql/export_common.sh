#!/usr/bin/env bash

export WALG_MYSQL_DATASOURCE_NAME=sbtest:@/sbtest
export WALG_MYSQL_BINLOG_SRC=${MYSQLDATA}
export WALG_MYSQL_BINLOG_DST=${MYSQLDATA}
export WALG_STREAM_CREATE_COMMAND="xtrabackup --backup \
    --stream=xbstream \
    --user=sbtest \
    --host=localhost \
    --parallel=2 \
    --datadir=${MYSQLDATA}"
export WALG_STREAM_RESTORE_COMMAND="xbstream -x -C ${MYSQLDATA}"
export WALG_MYSQL_BACKUP_PREPARE_COMMAND="xtrabackup --prepare --target-dir=${MYSQLDATA}"
