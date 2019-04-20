#!/bin/sh
set -e -x

export WALE_S3_PREFIX=s3://mysqlbinlogpushbucket
export WALG_MYSQL_DATASOURCE_NAME=sbtest:@/sbtest
export WALG_MYSQL_BINLOG_DST=${MYSQLDATA}

echo "CREATE DATABASE sbtest;" > /etc/mysql/init.sql
echo "CREATE USER sbtest@localhost;" >> /etc/mysql/init.sql
echo "GRANT ALL PRIVILEGES ON *.* TO sbtest@localhost;" >> /etc/mysql/init.sql

echo "[mysqld]" >> /etc/mysql/my.cnf
echo "datadir = ${MYSQLDATA}" >> /etc/mysql/my.cnf
echo "server_id = 1" >> /etc/mysql/my.cnf
echo "binlog_format = ROW" >> /etc/mysql/my.cnf
echo "log_bin = ${MYSQLDATA}/mysql-bin.log" >> /etc/mysql/my.cnf

mysqld --initialize --init-file=/etc/mysql/init.sql

service mysql start
xtrabackup --backup \
           --stream=xbstream \
           --user=sbtest \
           --host=localhost \
           --parallel=2 \
           --datadir=${MYSQLDATA} | wal-g stream-push

mysql -u sbtest -h localhost -e "FLUSH LOGS";
find /var/lib/mysql -printf "%f\n" | grep "mysql-bin" | sort | tail -n +2 > /tmp/mysql-bin1.index

wal-g binlog-push ${MYSQLDATA}
sleep 1
export WALG_MYSQL_BINLOG_END_TS=`date --rfc-3339=ns | sed 's/ /T/'`

pkill -9 mysql
rm -rf ${MYSQLDATA}

mkdir ${MYSQLDATA}
wal-g stream-fetch LATEST | xbstream -x -C ${MYSQLDATA}
chown -R mysql:mysql ${MYSQLDATA}
service mysql start

find /var/lib/mysql -printf "%f\n" | grep "mysql-bin" | sort > /tmp/mysql-bin2.index

diff /tmp/mysql-bin1.index /tmp/mysql-bin2.index

pkill -9 mysql
rm -rf ${MYSQLDATA}
echo "Mysql binlog-push test success!!!!!!"
