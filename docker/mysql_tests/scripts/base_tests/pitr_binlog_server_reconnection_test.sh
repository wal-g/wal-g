#!/bin/sh
set -e -x

. /usr/local/export_common.sh

s3cmd mb s3://mysql_pitr_binlogserver_reconnection_bucket || true
export WALE_S3_PREFIX=s3://mysql_pitr_binlogserver_reconnection_bucket
export WALG_MYSQL_BINLOG_SERVER_HOST="localhost"
export WALG_MYSQL_BINLOG_SERVER_PORT=9306
export WALG_MYSQL_BINLOG_SERVER_USER="walg"
export WALG_MYSQL_BINLOG_SERVER_PASSWORD="walgpwd"
export WALG_MYSQL_BINLOG_SERVER_ID=99
export WALG_MYSQL_BINLOG_SERVER_REPLICA_SOURCE="sbtest@tcp(127.0.0.1:3306)/sbtest"

mysqld --initialize --init-file=/etc/mysql/init.sql
service mysql start

wal-g backup-push

mysql -e "CREATE TABLE sbtest.pitr(id VARCHAR(64), ts DATETIME)"
mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr01', NOW())"
mysql -e "FLUSH LOGS"
wal-g binlog-push

for i in $(seq 1 500); do
    mysql -e "INSERT INTO sbtest.pitr VALUES('batch_$i', NOW())"
    if [ $((i % 100)) -eq 0 ]; then
        mysql -e "FLUSH LOGS"
        wal-g binlog-push
    fi
done

sleep 1
DT1=$(date3339)
sleep 1

mysql -e "INSERT INTO sbtest.pitr VALUES('after_cutoff', NOW())"
mysql -e "FLUSH LOGS"
wal-g binlog-push

mysql_kill_and_clean_data
wal-g backup-fetch LATEST
chown -R mysql:mysql $MYSQLDATA
service mysql start || (cat /var/log/mysql/error.log && false)
mysql_set_gtid_purged

BINLOG_SERVER_LOG=/tmp/binlog_server_reconnect.log

WALG_LOG_LEVEL="DEVEL" wal-g binlog-server --since LATEST --until "$DT1" 2>&1 | tee $BINLOG_SERVER_LOG &
walg_pid=$!

sleep 3

(
  for i in $(seq 1 2000); do
      mysql -e "INSERT INTO sbtest.pitr VALUES('live_$i', NOW())" || true
      if [ $((i % 200)) -eq 0 ]; then
          mysql -e "FLUSH LOGS"
          wal-g binlog-push
      fi
      sleep 0.2
  done
) &

mysql -e "STOP SLAVE"
mysql -e "SET GLOBAL SERVER_ID = 123"
mysql -e "CHANGE MASTER TO MASTER_HOST='127.0.0.1', MASTER_PORT=9306, MASTER_USER='walg', MASTER_PASSWORD='walgpwd', MASTER_AUTO_POSITION=1"
mysql -e "START SLAVE"

sleep 5
echo "Checking that replication has started..."
mysql -e "SHOW SLAVE STATUS\G" | grep -E 'Slave_IO_|Slave_SQL_|Last_IO_Error'

echo "Simulating network connection loss..."
iptables -A INPUT -p tcp --dport 9306 -j DROP
iptables -A OUTPUT -p tcp --sport 9306 -j DROP

sleep 10
echo "Network blocked, checking replication state:"
mysql -e "SHOW SLAVE STATUS\G" | grep -E 'Slave_IO_|Slave_SQL_|Last_IO_Error'

echo "Restoring network..."
iptables -D INPUT -p tcp --dport 9306 -j DROP
iptables -D OUTPUT -p tcp --sport 9306 -j DROP

sleep 20
echo "Checking replication after reconnect..."
mysql -e "SHOW SLAVE STATUS\G" | grep -E 'Slave_IO_|Slave_SQL_|Last_IO_Error'

SLAVE_IO_RUNNING=$(mysql -e "SHOW SLAVE STATUS\G" | grep "Slave_IO_Running: Yes" | wc -l)
if [ "$SLAVE_IO_RUNNING" -eq 1 ]; then
    echo "Replication restored successfully after reconnect"
else
    echo "ERROR: Replication did not recover"
    mysql -e "SHOW SLAVE STATUS\G"
    exit 1
fi

echo "Waiting for wal-g to complete..."
wait $walg_pid || true

CONN_COUNT=$(grep -c 'connection accepted from' "$BINLOG_SERVER_LOG" || true)
RECONNECT_COUNT=$(grep -c 'Returning existing streamer for reconnection' "$BINLOG_SERVER_LOG" || true)

echo "Connections accepted: $CONN_COUNT"
echo "Reconnections detected: $RECONNECT_COUNT"

if [ "$CONN_COUNT" -ge 2 ] || [ "$RECONNECT_COUNT" -ge 1 ]; then
    echo "Reconnection behavior verified"
else
    echo "WARNING: no reconnection detected in logs"
fi

echo "Test completed successfully!"