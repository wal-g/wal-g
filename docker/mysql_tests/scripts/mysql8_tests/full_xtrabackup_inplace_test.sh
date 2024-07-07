#!/bin/sh
set -e -x

. /usr/local/export_common.sh


export WALE_S3_PREFIX=s3://mysql8_full_xtrabackup_xbtool_bucket
export WALG_COMPRESSION_METHOD=zstd
export WALG_MYSQL_DATA_DIR="${MYSQLDATA}"

export WALG_STREAM_CREATE_COMMAND="xtrabackup --backup \
    --stream=xbstream \
    --user=sbtest \
    --host=localhost \
    --parallel=2 \
    --datadir=${MYSQLDATA}"
unset WALG_STREAM_RESTORE_COMMAND
export WALG_MYSQL_BACKUP_PREPARE_COMMAND="xtrabackup --prepare --target-dir=${MYSQLDATA}"

mysqld --initialize --init-file=/etc/mysql/init.sql

service mysql start

sysbench --table-size=10 prepare
sysbench --time=5 run

mysql -e 'FLUSH LOGS'

mysqldump sbtest > /tmp/dump_before_backup

wal-g backup-push

mysql_kill_and_clean_data

wal-g backup-fetch LATEST --use-xbtool-extract

chown -R mysql:mysql $MYSQLDATA

service mysql start || (cat /var/log/mysql/error.log && false)

mysql_set_gtid_purged

mysqldump sbtest > /tmp/dump_after_restore

diff /tmp/dump_before_backup /tmp/dump_after_restore
