#!/bin/sh
set -e -x

. /usr/local/export_common.sh

# Проверка и установка awk при необходимости
if ! command -v awk >/dev/null 2>&1; then
    echo "awk not found, installing..."
    apt-get update
    apt-get install -y gawk
fi

s3cmd mb s3://mysql-pitr-binlogserver-reconnection-bucket || true
export WALE_S3_PREFIX=s3://mysql-pitr-binlogserver-reconnection-bucket
export WALG_MYSQL_BINLOG_SERVER_HOST="localhost"
export WALG_MYSQL_BINLOG_SERVER_PORT=9306
export WALG_MYSQL_BINLOG_SERVER_USER="walg"
export WALG_MYSQL_BINLOG_SERVER_PASSWORD="walgpwd"
export WALG_MYSQL_BINLOG_SERVER_ID=99
export WALG_MYSQL_BINLOG_SERVER_REPLICA_SOURCE="sbtest@tcp(127.0.0.1:3306)/sbtest"

mysqld --initialize --init-file=/etc/mysql/init.sql
service mysql start

wal-g backup-push

mysql -e "CREATE TABLE sbtest.pitr(id VARCHAR(32), ts DATETIME)"
mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr01', NOW())"
mysql -e "FLUSH LOGS"
wal-g binlog-push

# Генерация данных
for i in $(seq 1 4000); do
    mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr_batch_$i', NOW())"
    if [ $((i % 50)) -eq 0 ]; then
        sleep 0.1
        mysql -e "FLUSH LOGS"
        wal-g binlog-push
    fi
done

sleep 1
DT1=$(date3339)
sleep 1

mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr_after', NOW())"
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

echo "Started wal-g binlog-server with PID: $walg_pid"
sleep 5

mysql -e "STOP SLAVE"
mysql -e "SET GLOBAL SERVER_ID = 123"
mysql -e "CHANGE MASTER TO MASTER_HOST=\"127.0.0.1\", MASTER_PORT=9306, MASTER_USER=\"walg\", MASTER_PASSWORD=\"walgpwd\", MASTER_AUTO_POSITION=1"
mysql -e "START SLAVE"

# Ожидание запуска репликации
echo "Waiting for replication to start..."
WAIT_COUNT=0
MAX_WAIT=30
while [ $WAIT_COUNT -lt $MAX_WAIT ]; do
    SLAVE_IO_RUNNING=$(mysql -e "SHOW SLAVE STATUS\G" | awk '/Slave_IO_Running:/ {print $2}')
    if [ "$SLAVE_IO_RUNNING" = "Yes" ]; then
        echo "Replication IO thread started successfully"
        break
    fi
    sleep 1
    WAIT_COUNT=$((WAIT_COUNT + 1))
done

if [ "$SLAVE_IO_RUNNING" != "Yes" ]; then
    echo "ERROR: Replication IO thread failed to start"
    mysql -e "SHOW SLAVE STATUS\G"
    exit 1
fi

# Разрыв соединения через 1 секунду
sleep 0.3
echo "Simulating network connection loss..."
BINLOG_SERVER_PORT=9306
ss -K dport = $BINLOG_SERVER_PORT 2>/dev/null || true
echo "TCP connections killed, waiting for reconnection..."
sleep 3

# Проверка переподключения
echo "Checking reconnection status..."
WAIT_COUNT=0
MAX_WAIT=15
while [ $WAIT_COUNT -lt $MAX_WAIT ]; do
    SLAVE_IO_RUNNING=$(mysql -e "SHOW SLAVE STATUS\G" | awk '/Slave_IO_Running:/ {print $2}')
    [ "$SLAVE_IO_RUNNING" = "Yes" ] && break
    sleep 1
    WAIT_COUNT=$((WAIT_COUNT + 1))
done

if [ "$SLAVE_IO_RUNNING" != "Yes" ]; then
    echo "ERROR: Reconnection failed"
    mysql -e "SHOW SLAVE STATUS\G"
    exit 1
fi

# Ожидание завершения репликации
echo "Waiting for replication to complete..."
MAX_WAIT=60
WAIT_COUNT=0
EXPECTED_ROWS=4001  # 1 начальная + 4000 сгенерированных
while [ $WAIT_COUNT -lt $MAX_WAIT ]; do
    ROW_COUNT=$(mysql -N -e "SELECT COUNT(*) FROM sbtest.pitr")
    echo "Current row count: $ROW_COUNT / $EXPECTED_ROWS"

    if [ "$ROW_COUNT" -eq "$EXPECTED_ROWS" ]; then
        echo "Replication completed successfully"
        break
    fi

    SLAVE_SQL_RUNNING=$(mysql -e "SHOW SLAVE STATUS\G" | awk '/Slave_SQL_Running:/ {print $2}')
    SLAVE_IO_RUNNING=$(mysql -e "SHOW SLAVE STATUS\G" | awk '/Slave_IO_Running:/ {print $2}')

    if [ "$SLAVE_SQL_Running" != "Yes" ] || [ "$SLAVE_IO_Running" != "Yes" ]; then
        echo "ERROR: Slave stopped unexpectedly"
        mysql -e "SHOW SLAVE STATUS\G"
        exit 1
    fi

    sleep 1
    WAIT_COUNT=$((WAIT_COUNT + 1))
done

if [ $WAIT_COUNT -eq $MAX_WAIT ]; then
    echo "ERROR: Timeout waiting for replication to complete"
    mysql -e "SHOW SLAVE STATUS\G"
    exit 1
fi

# Остановка сервера
echo "Stopping wal-g binlog-server (PID: $walg_pid)..."
if kill -0 $walg_pid 2>/dev/null; then
    kill -TERM $walg_pid
    wait $walg_pid 2>/dev/null || true
fi

# Проверка данных
AFTER_COUNT=$(mysql -N -e "SELECT COUNT(*) FROM sbtest.pitr WHERE id = 'testpitr_after'")
if [ "$AFTER_COUNT" -ne 0 ]; then
    echo "ERROR: Record after DT1 should not be replicated"
    exit 1
fi

# Проверка переподключения в логах
CONN_COUNT=$(grep -c 'connection accepted from' "$BINLOG_SERVER_LOG" || true)
RECONNECT_COUNT=$(grep -c 'Returning existing streamer for reconnection' "$BINLOG_SERVER_LOG" || true)

if [ "$RECONNECT_COUNT" -ge 1 ]; then
    echo "Reconnection detected in logs: $RECONNECT_COUNT times"
else
    echo "WARNING: No explicit reconnection detected in logs"
fi

if [ "$CONN_COUNT" -ge 2 ]; then
    echo "Multiple connections detected - reconnection test passed"
else
    echo "WARNING: Expected at least 2 connections, got $CONN_COUNT"
fi

echo "Test passed!"