#!/bin/sh
set -e -x

. /usr/local/export_common.sh

export WALE_S3_PREFIX=s3://mysqlbinlogpushbucket
export WALG_COMPRESSION_METHOD=lz4

mysqld --initialize --init-file=/etc/mysql/init.sql

service mysql start
wal-g backup-push

mysql -u sbtest -h localhost -e "FLUSH LOGS";
sysbench /usr/share/sysbench/oltp_insert.lua --table-size=10 prepare
sysbench /usr/share/sysbench/oltp_insert.lua --table-size=10 run
sleep 10
mysql -u sbtest -h localhost -e "FLUSH LOGS";
find "${MYSQLDATA}" -printf "%f\n" | grep "mysql-bin" | sort | tail -n +2 > /tmp/mysql-bin1.index
sleep 10
wal-g binlog-push && export WALG_MYSQL_BINLOG_END_TS=$(date --rfc-3339=ns | sed 's/ /T/') && kill_mysql_and_cleanup_data

mkdir "${MYSQLDATA}"
wal-g backup-fetch LATEST
wal-g binlog-fetch --since LATEST

chown -R mysql:mysql "${MYSQLDATA}"
sort "${MYSQLDATA}"/binlogs_order > /tmp/sorted_binlogs_order
service mysql start

find /var/lib/mysql -printf "%f\n" | grep "mysql-bin" | sort > /tmp/mysql-bin2.index

diff /tmp/mysql-bin1.index /tmp/mysql-bin2.index
diff "${MYSQLDATA}"/binlogs_order /tmp/sorted_binlogs_order

