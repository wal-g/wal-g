#!/usr/bin/env bash

# common wal-g settings
export WALG_MYSQL_DATASOURCE_NAME=sbtest:@/sbtest
export WALG_STREAM_CREATE_COMMAND="xtrabackup --backup \
    --stream=xbstream \
    --user=sbtest \
    --host=localhost \
    --parallel=2 \
    --datadir=${MYSQLDATA}"

export WALG_STREAM_RESTORE_COMMAND="xbstream -x -C ${MYSQLDATA}"
export WALG_MYSQL_BACKUP_PREPARE_COMMAND="xtrabackup --prepare --target-dir=${MYSQLDATA}"
# shellcheck disable=SC2016
export WALG_MYSQL_BINLOG_REPLAY_COMMAND='mysqlbinlog --stop-datetime="$WALG_MYSQL_BINLOG_END_TS" "$WALG_MYSQL_CURRENT_BINLOG" | mysql'
export WALG_MYSQL_BINLOG_DST=/tmp


. /usr/local/export_test_funcs.sh