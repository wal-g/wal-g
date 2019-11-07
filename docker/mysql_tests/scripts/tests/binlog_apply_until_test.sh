#!/bin/sh
set -e -x

. /usr/local/export_common.sh
export WALE_S3_PREFIX=s3://mysqlbinlogapplyuntilbucket
export WALG_STREAM_RESTORE_COMMAND="xtrabackup --prepare --target-dir=${MYSQLDATA}"
export WALG_LOG_APPLY_COMMAND=/root/testtools/test_apply.sh

add_test_data() {
  mysql -u sbtest -h localhost -e "
  USE sbtest;
  CREATE TABLE example ( id smallint unsigned not null auto_increment, name varchar(20) not null, constraint pk_example primary key (id) );
  INSERT INTO example ( id, name ) VALUES ( null, 'Sample data' );
  "
}

check_test_additional_data() {
  if [ -z "$(mysql -u sbtest -h localhost -e "USE sbtest; SHOW TABLES like 'example';")" ]; then
    echo no data!!!
    return 1
  fi;
  return 0
}

mysqld --initialize --init-file=/etc/mysql/init.sql

service mysql start
wal-g backup-push
export dtime1=$(date --rfc-3339=ns | sed 's/ /T/')
UNTILL1=$(date "+%Y-%m-%dT%H:%M:%S")
sleep 2s
mysql -u sbtest -h localhost -e 'FLUSH LOGS'
add_test_data
sleep 2s
mysql -u sbtest -h localhost -e 'FLUSH LOGS'
wal-g binlog-push
export dtime2=$(date --rfc-3339=ns | sed 's/ /T/')
UNTILL2=$(date "+%Y-%m-%dT%H:%M:%S")

kill_mysql_and_cleanup_data

mkdir "${MYSQLDATA}"
wal-g backup-fetch LATEST | xbstream -x -C "${MYSQLDATA}"
chown -R mysql:mysql "${MYSQLDATA}"

sleep 10
service mysql start || cat /var/log/mysql/error.log

export UNTILL=${UNTILL2}
wal-g binlog-fetch --since LATEST --until "${dtime2}" --apply

check_test_additional_data

kill_mysql_and_cleanup_data

mkdir "${MYSQLDATA}"
wal-g backup-fetch LATEST | xbstream -x -C "${MYSQLDATA}"
chown -R mysql:mysql "${MYSQLDATA}"

sleep 10
service mysql start || cat /var/log/mysql/error.log

export UNTILL=${UNTILL1}
wal-g binlog-fetch --since LATEST --until "${dtime1}" --apply

if check_test_additional_data; then
  return 1
fi