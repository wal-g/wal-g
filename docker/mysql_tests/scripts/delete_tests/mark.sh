#!/bin/sh
set -e -x

. /usr/local/export_common.sh

export WALE_S3_PREFIX=s3://mysqlmarkbucket


# initialize mysql
mysqld --initialize --init-file=/etc/mysql/init.sql
service mysql start
sysbench --table-size=10 prepare
mysql -e "FLUSH LOGS"

# backup
sysbench --time=3 run
wal-g backup-push
sysbench --time=3 run
mysql -e "FLUSH LOGS"
wal-g binlog-push
sleep 1

wal-g backup-list
BACKUP=$(wal-g backup-list | awk 'NR==2{print $1}')

wal-g backup-mark -b "$BACKUP"

if wal-g delete everything --confirm; then
  echo '
  permanent backups deleted!!!
'
  exit 1
fi

wal-g backup-list

wal-g backup-mark -b "$BACKUP" -i

wal-g delete everything --confirm

if wal-g backup-mark "noexitsstinfbackuonme"; then
    echo '
    backup name successfully marked not-exsting backup
    '
    exit 1
fi
