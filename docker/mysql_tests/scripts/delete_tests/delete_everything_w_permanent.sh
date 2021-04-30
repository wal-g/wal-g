#!/bin/sh
set -e -x

. /usr/local/export_common.sh

export WALE_S3_PREFIX=s3://mysqldeleteeverythingpermanent

# initialize mysql
mysqld --initialize --init-file=/etc/mysql/init.sql
service mysql start
sysbench --table-size=10 prepare
mysql -e "FLUSH LOGS"

# permanent backup
sysbench --time=3 run
wal-g backup-push --permanent
sysbench --time=3 run
mysql -e "FLUSH LOGS"
wal-g binlog-push
sleep 1

# non-permanent backup
sysbench --time=3 run
wal-g backup-push
sysbench --time=3 run
mysql -e "FLUSH LOGS"
wal-g binlog-push
sleep 1

test "3" -eq "$(wal-g backup-list | wc -l)"

# assert that WAL-G can't delete permanent backups without the FORCE flag
if wal-g delete everything --confirm; then
    echo '
    wal-g delete everything deleted permanent backup without the FORCE flag
    '
    exit 1
fi

# assert that WAL-G can delete permanent backup with the FORCE flag
wal-g delete everything FORCE --confirm

# check that we don't have any backups left after delete
test "1" -eq "$(wal-g backup-list | wc -l)"
