#!/bin/sh
set -e -x

export WALE_S3_PREFIX=s3://mysqldeleteendtoendbucket
export WALG_MYSQL_DATASOURCE_NAME=sbtest:@/sbtest
export WALG_MYSQL_BINLOG_SRC=${MYSQLDATA}
export WALG_MYSQL_BINLOG_DST=${MYSQLDATA}

mysqld --initialize --init-file=/etc/mysql/init.sql

service mysql start

sysbench /usr/share/sysbench/oltp_insert.lua --table-size=10 prepare
for i in $(seq 1 4);
do
    sysbench /usr/share/sysbench/oltp_insert.lua --table-size=10 run

    if [ $i -eq 3 ];
    then
        sleep 3
        mysqldump -u sbtest --all-databases --lock-tables=false | head -n -1  > /tmp/dump_backup
        sleep 3
    fi

    xtrabackup --backup \
           --stream=xbstream \
           --user=sbtest \
           --host=localhost \
           --parallel=2 \
           --datadir=${MYSQLDATA} | wal-g stream-push
done

wal-g backup-list

target_backup_name=`wal-g backup-list | tail -n 2 | head -n 1 | cut -f 1 -d " "`

wal-g delete before FIND_FULL $target_backup_name --confirm

wal-g backup-list

sleep 3

pkill -9 mysql
rm -rf ${MYSQLDATA}

mkdir ${MYSQLDATA}
wal-g stream-fetch $target_backup_name | xbstream -x -C ${MYSQLDATA}
chown -R mysql:mysql ${MYSQLDATA}

sleep 10
service mysql start || cat /var/log/mysql/error.log

sleep 10
mysqldump -u sbtest --all-databases --lock-tables=false | head -n -1 > /tmp/dump_restored
sleep 10

diff /tmp/dump_backup /tmp/dump_restored

pkill -9 mysql
rm -rf ${MYSQLDATA}
echo "Mysql delete end to end test success!!!!!!"
