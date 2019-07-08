#!/bin/sh
set -e -x

export WALE_S3_PREFIX=s3://mysqlbinlogpushbucket
export WALG_MYSQL_DATASOURCE_NAME=sbtest:@/sbtest
export WALG_MYSQL_BINLOG_SRC=${MYSQLDATA}
export WALG_MYSQL_BINLOG_DST=${MYSQLDATA}

mysqld --initialize --init-file=/etc/mysql/init.sql

service mysql start
xtrabackup --backup \
           --stream=xbstream \
           --user=sbtest \
           --host=localhost \
           --parallel=2 \
           --datadir=${MYSQLDATA} | wal-g stream-push

mysql -u sbtest -h localhost -e "FLUSH LOGS";
sysbench /usr/share/sysbench/oltp_insert.lua --table-size=10 prepare
sysbench /usr/share/sysbench/oltp_insert.lua --table-size=10 run
mysql -u sbtest -h localhost -e "FLUSH LOGS";
find ${MYSQLDATA} -printf "%f\n" | grep "mysql-bin" | sort | tail -n +2 > /tmp/mysql-bin1.index

wal-g binlog-push
sleep 1
export WALG_MYSQL_BINLOG_END_TS=`date --rfc-3339=ns | sed 's/ /T/'`

pkill -9 mysql
rm -rf ${MYSQLDATA}

mkdir ${MYSQLDATA}
wal-g stream-fetch LATEST | xbstream -x -C ${MYSQLDATA}
chown -R mysql:mysql ${MYSQLDATA}
sort ${MYSQLDATA}/binlogs_order > /tmp/sorted_binlogs_order
service mysql start

find /var/lib/mysql -printf "%f\n" | grep "mysql-bin" | sort > /tmp/mysql-bin2.index

diff /tmp/mysql-bin1.index /tmp/mysql-bin2.index
diff ${MYSQLDATA}/binlogs_order /tmp/sorted_binlogs_order

pkill -9 mysql
rm -rf ${MYSQLDATA}
echo "Mysql binlog-push test success!!!!!!"
