#!/bin/sh
set -e -x

. /usr/local/export_common.sh

export WALE_S3_PREFIX=s3://mysqldeletebinlogs

# initialize mysql
mysqld --initialize --init-file=/etc/mysql/init.sql
service mysql start
sysbench --table-size=10 prepare
mysql -e "FLUSH LOGS"

# first backup
sysbench --time=3 run
wal-g backup-push
sysbench --time=3 run
mysql -e "FLUSH LOGS"
wal-g binlog-push
sleep 1

mysqldump sbtest > /tmp/dump_1.sql
wal-g binlog-list
BINLOGS=$(wal-g binlog-list | awk 'NR==2{print $1}')
echo $BINLOGS



