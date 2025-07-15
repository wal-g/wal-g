#!/bin/sh
set -e -x

. /usr/local/export_common.sh

export WALE_S3_PREFIX=s3://mysql_pitr_binlogserver_bucket
export WALG_MYSQL_BINLOG_SERVER_HOST="localhost"
export WALG_MYSQL_BINLOG_SERVER_PORT=9306
export WALG_MYSQL_BINLOG_SERVER_USER="walg"
export WALG_MYSQL_BINLOG_SERVER_PASSWORD="walgpwd"
export WALG_MYSQL_BINLOG_SERVER_ID=99
export WALG_MYSQL_BINLOG_SERVER_REPLICA_SOURCE="sbtest@tcp(127.0.0.1:3306)/sbtest"

mysqld --initialize --init-file=/etc/mysql/init.sql
service mysql start

# first full backup
wal-g backup-push

mysql -e "CREATE TABLE sbtest.pitr(id VARCHAR(32), ts DATETIME)"
mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr01', NOW())"
mysql -e "FLUSH LOGS"
wal-g binlog-push
mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr02', NOW())"
mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr03', NOW())"
sleep 1
DT1=$(date3339)
sleep 1
mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr04', NOW())"
mysql -e "FLUSH LOGS"
wal-g binlog-push


# pitr restore across full backup
mysql_kill_and_clean_data
wal-g backup-fetch LATEST
chown -R mysql:mysql $MYSQLDATA
service mysql start || (cat /var/log/mysql/error.log && false)
mysql_set_gtid_purged

WALG_LOG_LEVEL="DEVEL" wal-g binlog-server --since LATEST --until "$DT1" &
walg_pid=$!

sleep 3
mysql -e "STOP SLAVE"
mysql -e "SET GLOBAL SERVER_ID = 123"
mysql -e "CHANGE MASTER TO MASTER_HOST=\"127.0.0.1\", MASTER_PORT=9306, MASTER_USER=\"walg\", MASTER_PASSWORD=\"walgpwd\", MASTER_AUTO_POSITION=1"
mysql -e "START SLAVE"

wait $walg_pid

mysqldump sbtest > /tmp/dump_after_pitr
grep -w 'testpitr01' /tmp/dump_after_pitr
grep -w 'testpitr02' /tmp/dump_after_pitr
grep -w 'testpitr03' /tmp/dump_after_pitr
! grep -w 'testpitr04' /tmp/dump_after_pitr
