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

# Вставка данных
mysql -e "CREATE TABLE sbtest.pitr(id VARCHAR(32), ts DATETIME)"
mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr01', NOW())"
mysql -e "FLUSH LOGS"
wal-g binlog-push

mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr02', NOW())"
mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr03', NOW())"
sleep 1
DT1=$(date3339)  # Точка PITR — после 02, 03
sleep 1
mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr04', NOW())"
mysql -e "FLUSH LOGS"
wal-g binlog-push

# Восстановление: fetch backup
mysql_kill_and_clean_data
wal-g backup-fetch LATEST
chown -R mysql:mysql $MYSQLDATA
service mysql start || (cat /var/log/mysql/error.log && false)
mysql_set_gtid_purged

FUTURE_DT=$(date --rfc-3339=ns | sed 's/ /T/' | sed 's/\.[0-9]*+/+00:00/')
WALG_LOG_LEVEL="DEVEL" wal-g binlog-server --since LATEST --until "$FUTURE_DT" > "$WALG_LOG_FILE" 2>&1 &
WALG_PID=$!

sleep 5

netstat -tuln | grep 9306 || (echo "❌ binlog-server is not listening on 9306" && exit 1)

mysql -e "STOP SLAVE"
mysql -e "SET GLOBAL SERVER_ID = 123"
mysql -e "CHANGE MASTER TO MASTER_HOST='127.0.0.1', MASTER_PORT=9306, MASTER_USER='walg', MASTER_PASSWORD='walgpwd', MASTER_AUTO_POSITION=1"
mysql -e "START SLAVE"

sleep 10

grep "connection accepted" "$WALG_LOG_FILE" | grep -q "127.0.0.1" \
  && echo "✅ First connection accepted" \
  || (echo "❌ No first connection in logs" && cat "$WALG_LOG_FILE" && exit 1)

mysql -e "STOP SLAVE"
sleep 3
mysql -e "START SLAVE"

sleep 10

if grep "connection accepted" "$WALG_LOG_FILE" | grep -c "127.0.0.1" | grep -q "2"; then
  echo "✅ Second connection accepted (reconnection)"
else
  echo "❌ Expected two connections, got:"
  grep "connection accepted" "$WALG_LOG_FILE"
  cat "$WALG_LOG_FILE"
  exit 1
fi

grep "Returning existing streamer for reconnection" "$WALG_LOG_FILE" \
  && echo "✅ Reused existing streamer on reconnection" \
  || (echo "❌ Expected reconnection reuse" && cat "$WALG_LOG_FILE" && exit 1)

kill $WALG_PID || true
wait $WALG_PID || echo "wal-g binlog-server exited"

mysqldump sbtest > /tmp/dump_after_pitr

grep -w 'testpitr01' /tmp/dump_after_pitr
grep -w 'testpitr02' /tmp/dump_after_pitr
grep -w 'testpitr03' /tmp/dump_after_pitr
# testpitr04 не должно быть, если мы хотим PITR
! grep -w 'testpitr04' /tmp/dump_after_pitr

mysql -e "SHOW SLAVE STATUS FOR CHANNEL 'master'\G" || true

echo "✅ Test passed: binlog-server handled reconnection correctly and resumed replication."
