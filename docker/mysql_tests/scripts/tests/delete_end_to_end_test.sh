#!/bin/sh
set -e -x

. /usr/local/export_common.sh

export WALE_S3_PREFIX=s3://mysqldeleteendtoendbucket
export WALG_STREAM_RESTORE_COMMAND="xtrabackup --prepare --target-dir=${MYSQLDATA}"


mysqld --initialize --init-file=/etc/mysql/init.sql

service mysql start

sysbench /usr/share/sysbench/oltp_insert.lua --table-size=10 prepare
for i in $(seq 1 4);
do
    sysbench /usr/share/sysbench/oltp_insert.lua --table-size=10 run

    if [ "$i" -eq 3 ];
    then
        sleep 3
        # mv ${MYSQLDATA}/mysql /tmp/mysql
        # mv /tmp/mysql ${MYSQLDATA}/mysql
        # allows to avoid flap in test, because stats in mysql db can be different after dump

        mv "${MYSQLDATA}"/mysql /tmp/mysql
        mysqldump -u sbtest --all-databases --lock-tables=false | head -n -1  > /tmp/dump_backup
        sleep 3
        mv /tmp/mysql "${MYSQLDATA}"/mysql
    fi

    wal-g backup-push
done

wal-g backup-list

target_backup_name=$(wal-g backup-list | tail -n 2 | head -n 1 | cut -f 1 -d " ")

wal-g delete before FIND_FULL "$target_backup_name" --confirm
wal-g backup-list && sleep 3

kill_mysql_and_cleanup_data

mkdir "${MYSQLDATA}"
wal-g backup-fetch "$target_backup_name" | xbstream -x -C "${MYSQLDATA}"
chown -R mysql:mysql "${MYSQLDATA}"

sleep 10
service mysql start || cat /var/log/mysql/error.log

sleep 10
mv "${MYSQLDATA}"/mysql /tmp/mysql
mysqldump -u sbtest --all-databases --lock-tables=false | head -n -1 > /tmp/dump_restored
sleep 10
mv /tmp/mysql "${MYSQLDATA}"/mysql
diff -I 'SET @@GLOBAL.GTID_PURGED' /tmp/dump_backup /tmp/dump_restored

