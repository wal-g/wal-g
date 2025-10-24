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

WALG_LOG_FILE="/tmp/walg_binlog_server.log"

mysqld --initialize --init-file=/etc/mysql/init.sql
service mysql start

wal-g backup-push

mysql -e "CREATE TABLE sbtest.pitr(id VARCHAR(32), ts DATETIME)"
mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr01', NOW())"
mysql -e "FLUSH LOGS"
wal-g binlog-push

mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr02', NOW())"
mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr03', NOW())"
sleep 1
DT1=$(date --rfc-3339=ns | sed 's/ /T/')
sleep 1
mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr04', NOW())"
mysql -e "FLUSH LOGS"
wal-g binlog-push

mysql_kill_and_clean_data
wal-g backup-fetch LATEST
chown -R mysql:mysql $MYSQLDATA
service mysql start || (cat /var/log/mysql/error.log && false)
mysql_set_gtid_purged

FUTURE_DT=$(date -u -d "+10 minutes" +"%Y-%m-%dT%H:%M:%S.000000000+00:00")
WALG_LOG_LEVEL="DEVEL" wal-g binlog-server --since LATEST --until "$FUTURE_DT" > "$WALG_LOG_FILE" 2>&1 &
WALG_PID=$!

echo "⏳ Waiting for wal-g binlog-server to start listening on 9306..."
timeout 30s bash -c "
  while ! grep -F 'Listening on 127.0.0.1:9306' '$WALG_LOG_FILE' > /dev/null 2>&1; do
    sleep 1
  done
" || (echo "❌ Timeout: wal-g did not start listening on 9306" && cat "$WALG_LOG_FILE" && exit 1)
echo "✅ wal-g binlog-server is listening on 127.0.0.1:9306"

mysql -e "STOP SLAVE"
mysql -e "SET GLOBAL SERVER_ID = 123"
mysql -e "CHANGE MASTER TO MASTER_HOST='127.0.0.1', MASTER_PORT=9306, MASTER_USER='walg', MASTER_PASSWORD='walgpwd', MASTER_AUTO_POSITION=1"
mysql -e "START SLAVE"

sleep 5

grep "connection accepted" "$WALG_LOG_FILE" | grep -q "127.0.0.1" \
  && echo "✅ First connection accepted" \
  || (echo "❌ No first connection in logs" && cat "$WALG_LOG_FILE" && exit 1)

mysql -e "STOP SLAVE"
sleep 3
mysql -e "START SLAVE"

sleep 5

CONNECTION_COUNT=$(grep "connection accepted" "$WALG_LOG_FILE" | grep -c "127.0.0.1" || true)
if [ "$CONNECTION_COUNT" -ge 2 ]; then
  echo "✅ Second connection accepted (reconnection): $CONNECTION_COUNT total"
else
  echo "❌ Expected at least 2 connections, got: $CONNECTION_COUNT"
  cat "$WALG_LOG_FILE"
  exit 1
fi

grep "Returning existing streamer for reconnection" "$WALG_LOG_FILE" \
  && echo "✅ Reused existing streamer on reconnection — your fix works!" \
  || (echo "❌ Expected streamer reuse not found" && cat "$WALG_LOG_FILE" && exit 1)

echo "🛑 Killing wal-g binlog-server (PID: $WALG_PID)"
kill $WALG_PID || echo "wal-g already exited"
wait $WALG_PID || echo "wal-g exited with code $?"

mysqldump sbtest > /tmp/dump_after_pitr

grep -w 'testpitr01' /tmp/dump_after_pitr
grep -w 'testpitr02' /tmp/dump_after_pitr
grep -w 'testpitr03' /tmp/dump_after_pitr
! grep -w 'testpitr04' /tmp/dump_after_pitr

mysql -e "SHOW SLAVE STATUS FOR CHANNEL 'master'\G" || true

echo "✅ Test passed: binlog-server handled reconnection correctly and resumed replication."
