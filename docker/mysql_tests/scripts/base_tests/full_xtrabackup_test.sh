#!/bin/sh
set -e -x

. /usr/local/export_common.sh

export WALE_S3_PREFIX=s3://mysqlfullxtrabackupbucket


mysql_initialize_and_start

sysbench --table-size=10 prepare

sysbench --time=5 run

mysql -e 'FLUSH LOGS'

mysqldump sbtest > /tmp/dump_before_backup

wal-g backup-push

mysql_kill_and_clean_data

wal-g backup-fetch LATEST

chown -R mysql:mysql $MYSQLDATA

mysql_start

mysql_set_gtid_purged

mysqldump sbtest > /tmp/dump_after_restore

diff /tmp/dump_before_backup /tmp/dump_after_restore
