#!/bin/sh
set -e -x

. /usr/local/export_common.sh

export WALE_S3_PREFIX=s3://mysqllivereplay
export WALG_MYSQL_BINLOG_REPLAY_COMMAND='echo "Ok" >> "$WALG_MYSQL_CURRENT_BINLOG.ok" ; while [ ! -f "$WALG_MYSQL_CURRENT_BINLOG.in" ]; do sleep 1; done'
export WALG_MYSQL_BINLOG_DST="/tmp"

mysqld --initialize --init-file=/etc/mysql/init.sql
service mysql start
wal-g backup-push
sleep 1

mysql -e "CREATE TABLE sbtest.pitr(id VARCHAR(32), ts DATETIME)"
#  REPLAY_COMMAND may lag behind DOWNLOAD for 'binlogFetchAhead' (internal/databases/mysql/binlog_replay_handler.go)
#  so, we are making a lot of binlog files:
for idx in 1 2 3 4 5 6 7
do
mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr$idx', NOW())"
mysql -e "FLUSH LOGS"
wal-g binlog-push
done
sleep 1

wal-g binlog-replay --until "2030-01-01T00:00:00.000000000+00:00" --live-replay &

while [ ! -f "/tmp/mysql-bin.000001.ok" ]; do sleep 1; done

# wal-g should be blocked in REPLAY_COMMAND waiting for /tmp/mysql-bin.000002.in
mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr_last', NOW())"
mysql -e "FLUSH LOGS"
wal-g binlog-push
sleep 1

echo "proceed" > /tmp/mysql-bin.000001.in

for idx in 2 3 4 5 6 7 8 9
do
while [ ! -f "/tmp/mysql-bin.00000$idx.ok" ]; do sleep 1; done
echo "proceed" > "/tmp/mysql-bin.00000$idx.in"
done