#!/bin/sh
set -e -x

. /usr/local/export_common.sh

s3cmd mb s3://mysql_pitr_binlogserver_reconnection_bucket || true
export WALE_S3_PREFIX=s3://mysql_pitr_binlogserver_bucket
export WALG_MYSQL_BINLOG_SERVER_HOST="localhost"
export WALG_MYSQL_BINLOG_SERVER_PORT=9306
export WALG_MYSQL_BINLOG_SERVER_USER="walg"
export WALG_MYSQL_BINLOG_SERVER_PASSWORD="walgpwd"
export WALG_MYSQL_BINLOG_SERVER_ID=99
export WALG_MYSQL_BINLOG_SERVER_REPLICA_SOURCE="sbtest@tcp(127.0.0.1:3306)/sbtest"
export WALG_BINLOG_SERVER_KEEP_ALIVE=true

mysqld --initialize --init-file=/etc/mysql/init.sql
service mysql start
mysql -e "CREATE DATABASE IF NOT EXISTS sbtest;"
mysql -e "CREATE USER IF NOT EXISTS 'walg'@'%' IDENTIFIED BY 'walgpwd';"
mysql -e "GRANT REPLICATION SLAVE ON *.* TO 'walg'@'%'; FLUSH PRIVILEGES;"

wal-g backup-push

echo "Starting WAL-G binlog server..."
wal-g binlog-server --listen-addr=0.0.0.0:9306 --dir=/tmp/binlogs > /tmp/binlog_server.log 2>&1 &
BINLOG_SERVER_PID=$!

sleep 5
echo "Binlog server started with PID $BINLOG_SERVER_PID"

mysql -e "CHANGE MASTER TO
  MASTER_HOST='127.0.0.1',
  MASTER_PORT=9306,
  MASTER_USER='walg',
  MASTER_PASSWORD='walgpwd',
  MASTER_AUTO_POSITION=1;"

mysql -e "START SLAVE;"

sleep 5
mysql -e "SHOW SLAVE STATUS\G" | grep -E "Running|State" || true

echo "Initial binlog connections:"
ss -tnp | grep ":9306" || true

echo "Killing binlog-server connection to simulate disconnect..."
CONN_PID=$(ss -tnp | grep ":9306" | awk '{print $7}' | cut -d',' -f2 | cut -d'=' -f2 | head -n1 || true)
if [ -n "$CONN_PID" ]; then
  kill -9 "$CONN_PID" || true
else
  echo "No connection PID found for port 9306"
fi

echo "Waiting for MySQL to detect disconnect..."
sleep 30

echo "Binlog connections after 30s:"
ss -tnp | grep ":9306" || true

mysql -e "SHOW SLAVE STATUS\G" | grep -E "Running|State" || true

echo "=== Binlog server logs ==="
cat /tmp/binlog_server.log || true

if grep -q "connection accepted" /tmp/binlog_server.log; then
  COUNT=$(grep -c "connection accepted" /tmp/binlog_server.log)
  if [ "$COUNT" -ge 2 ]; then
    echo "✅ SUCCESS: MySQL reconnected to binlog-server ($COUNT connections)"
    exit 0
  else
    echo "❌ FAIL: Only one connection detected, no reconnect"
    exit 1
  fi
else
  echo "❌ FAIL: No connections detected at all"
  exit 1
fi