#!/bin/sh
set -e -x

. /usr/local/export_common.sh

export WALE_S3_PREFIX=s3://mysqlpermanentbackupbucket


mysqld --initialize --init-file=/etc/mysql/init.sql

service mysql start

sysbench --table-size=10 prepare

sysbench --time=5 run

mysql -e 'FLUSH LOGS'

mysqldump sbtest > /tmp/dump_before_backup

wal-g backup-push --permanent

mysql_kill_and_clean_data

# this should fail because the pushed backup is permanent
if wal-g delete everything --confirm; then
  echo '
  permanent backups deleted!!!
'
  exit 1
fi
