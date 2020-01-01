#!/bin/sh
set -e -x

. /usr/local/export_common.sh
export WALE_S3_PREFIX=s3://mysqlbinlogpushwhiles3downbucket
export WALG_COMPRESSION_METHOD=lz4

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

mysql -u sbtest -h localhost -e "FLUSH LOGS";
add_test_data
sleep 10
mysql -u sbtest -h localhost -e "FLUSH LOGS" && sleep 10

export UNTILL=$(date "+%Y-%m-%dT%H:%M:%S") && export WALG_MYSQL_BINLOG_END_TS=$(date --rfc-3339=ns | sed 's/ /T/') && sleep 20 && wal-g binlog-push  && kill_mysql_and_cleanup_data

mkdir "${MYSQLDATA}"
wal-g backup-fetch LATEST | xbstream -x -C "${MYSQLDATA}"


# start mysql && try to apply binlogs
chown -R mysql:mysql "${MYSQLDATA}"
service mysql start && wal-g binlog-fetch --since LATEST --until "${WALG_MYSQL_BINLOG_END_TS}" --apply

check_test_additional_data

