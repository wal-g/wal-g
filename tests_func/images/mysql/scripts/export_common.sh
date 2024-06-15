#!/usr/bin/env bash

# common wal-g settings
export AWS_ACCESS_KEY_ID="S3_ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="S3_SECRET_KEY"
export AWS_ENDPOINT="http://minio:9000"
export AWS_S3_FORCE_PATH_STYLE="true"

export WALG_COMPRESSION_METHOD=zstd
export WALG_DELTA_MAX_STEPS=6
export WALG_UPLOAD_CONCURRENCY=10
export WALG_DISK_RATE_LIMIT=41943040
export WALG_NETWORK_RATE_LIMIT=10485760
#export WALG_LOG_LEVEL=DEVEL


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
