#!/bin/sh
set -e -x

export WALE_S3_PREFIX=s3://mysqlbinlogpushbucket
export WALG_MYSQL_DATASOURCE_NAME=sbtest:@/sbtest
export WALG_MYSQL_BINLOG_SRC=${MYSQLDATA}
export WALG_MYSQL_BINLOG_DST="${MYSQLDATA}/.oldbinlogs"
export WALG_STREAM_CREATE_COMMAND="xtrabackup --backup \
    --stream=xbstream \
    --user=sbtest \
    --host=localhost \
    --parallel=2 \
    --datadir=${MYSQLDATA}"
export WALG_MYSQL_BINLOG_END_TS=$(date --rfc-3339=ns | sed 's/ /T/')
export WALG_STREAM_RESTORE_COMMAND="xbstream -x -C ${MYSQLDATA}"
export WALG_MYSQL_BINLOG_APPLY_COMMAND_PATH=/tmp/test_apply.sh

kill_mysql_and_cleanup_data() {
    pkill -9 mysqld
    rm -rf "${MYSQLDATA}"
}

add_test_data() {
  mysql -u sbtest -h localhost -e "
  CREATE DATABASE dbname;
  USE dbname;
  CREATE TABLE example ( id smallint unsigned not null auto_increment, name varchar(20) not null, constraint pk_example primary key (id) );
  INSERT INTO example ( id, name ) VALUES ( null, 'Sample data' );
  "
}

check_test_additional_data() {
  if [ -z "$(mysql -u sbtest -h localhost -e "USE dbname; SHOW TABLES like 'example';")" ] && [ $? -eq 0 ]; then
    return 0
  fi
  return 1
}

mysqld --initialize --init-file=/etc/mysql/init.sql

service mysql start

wal-g backup-push

dtime1=$(date --rfc-3339=ns | sed 's/ /T/')
sleep 2s
add_test_data
sleep 2s
wal-g binlog-push
dtime2=$(date --rfc-3339=ns | sed 's/ /T/')

kill_mysql_and_cleanup_data

mkdir "${MYSQLDATA}"
wal-g backup-fetch LATEST
mkdir "${MYSQLDATA}/.oldbinlogs"
chown -R mysql:mysql "${MYSQLDATA}"

sleep 10
service mysql start || cat /var/log/mysql/error.log

wal-g binlog-fetch --since LATEST --until "$dtime2"
ls -a "${WALG_MYSQL_BINLOG_DST}"
check_test_additional_data