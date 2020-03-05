#!/bin/sh
set -e -x

. /usr/local/export_common.sh

export WALE_S3_PREFIX=s3://mysqlbinlogreplaybucket
export WALG_MYSQL_BINLOG_REPLAY_COMMAND="mysqlbinlog -v - >> /tmp/binlog.sql"


mysqld --initialize --init-file=/etc/mysql/init.sql
service mysql start
wal-g backup-push
sleep 1

mysql -e "CREATE TABLE sbtest.pitr(id VARCHAR(32), ts DATETIME)"
mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr01', NOW())"
sleep 1

DT1=$(date3339)
sleep 1

mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr02', NOW())"
mysql -e "FLUSH LOGS"
wal-g binlog-push
mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr03', NOW())"
sleep 1

DT2=$(date3339)
sleep 1

mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr04', NOW())"
mysql -e "FLUSH LOGS"
wal-g binlog-push
sleep 1


# test first point
rm -rf /tmp/binlog.sql
wal-g binlog-replay --since LATEST --until "$DT1"
test -f /tmp/binlog.sql
grep -w 'testpitr01' /tmp/binlog.sql
! grep -w 'testpitr02' /tmp/binlog.sql
! grep -w 'testpitr03' /tmp/binlog.sql
! grep -w 'testpitr04' /tmp/binlog.sql


# test second point
rm -rf /tmp/binlog.sql
wal-g binlog-replay --since LATEST --until "$DT2"
test -f /tmp/binlog.sql
grep -w 'testpitr01' /tmp/binlog.sql
grep -w 'testpitr02' /tmp/binlog.sql
grep -w 'testpitr03' /tmp/binlog.sql
! grep -w 'testpitr04' /tmp/binlog.sql

# test whole history
rm -rf /tmp/binlog.sql
wal-g binlog-replay --since LATEST
test -f /tmp/binlog.sql
grep -w 'testpitr01' /tmp/binlog.sql
grep -w 'testpitr02' /tmp/binlog.sql
grep -w 'testpitr03' /tmp/binlog.sql
grep -w 'testpitr04' /tmp/binlog.sql
