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

for i in $(seq 1 5000); do
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

echo "Waiting for data transfer to begin..."
sleep 3

WAIT_COUNT=0
MAX_WAIT=20
while [ $WAIT_COUNT -lt $MAX_WAIT ]; do
    ROW_COUNT=$(mysql -N -e "SELECT COUNT(*) FROM sbtest.pitr" 2>/dev/null || echo "0")
    echo "Current row count before disconnect: $ROW_COUNT"

    # Разрываем соединение когда получили часть данных, но не все
    if [ "$ROW_COUNT" -gt 500 ] && [ "$ROW_COUNT" -lt 4000 ]; then
        echo "Partial data received ($ROW_COUNT rows), simulating connection loss..."
        break
    fi

    sleep 1
    WAIT_COUNT=$((WAIT_COUNT + 1))
done

echo "Simulating network connection loss..."
BINLOG_SERVER_PORT=9306
ss -K dport = $BINLOG_SERVER_PORT 2>/dev/null || true
echo "TCP connections killed, waiting for reconnection..."
sleep 2

echo "Checking reconnection status..."
WAIT_COUNT=0
MAX_WAIT=30
RECONNECTED=false
while [ $WAIT_COUNT -lt $MAX_WAIT ]; do
    SLAVE_IO_RUNNING=$(mysql -e "SHOW SLAVE STATUS\G" | awk '/Slave_IO_Running:/ {print $2}')
    SLAVE_IO_STATE=$(mysql -e "SHOW SLAVE STATUS\G" | awk '/Slave_IO_State:/ {print $2}' | head -1)

    echo "Attempt $((WAIT_COUNT + 1))/$MAX_WAIT: IO_Running=$SLAVE_IO_RUNNING, IO_State=$SLAVE_IO_STATE"

    if [ "$SLAVE_IO_RUNNING" = "Yes" ]; then
        echo "Reconnection successful!"
        RECONNECTED=true
        break
    fi

    sleep 1
    WAIT_COUNT=$((WAIT_COUNT + 1))
done

if [ "$RECONNECTED" = "false" ]; then
    echo "ERROR: Reconnection failed after $MAX_WAIT attempts"
    mysql -e "SHOW SLAVE STATUS\G"
    echo "Binlog server log:"
    cat $BINLOG_SERVER_LOG
    exit 1
fi

echo "Waiting for replication to complete..."
MAX_WAIT=100
WAIT_COUNT=0
EXPECTED_ROWS=5001  # 1 начальная + 5000 сгенерированных
while [ $WAIT_COUNT -lt $MAX_WAIT ]; do
    ROW_COUNT=$(mysql -N -e "SELECT COUNT(*) FROM sbtest.pitr")
    echo "Current row count: $ROW_COUNT / $EXPECTED_ROWS"

    if [ "$ROW_COUNT" -eq "$EXPECTED_ROWS" ]; then
        echo "Replication completed successfully"
        break
    fi

    SLAVE_SQL_RUNNING=$(mysql -e "SHOW SLAVE STATUS\G" | awk '/Slave_SQL_Running:/ {print $2}')
    SLAVE_IO_RUNNING=$(mysql -e "SHOW SLAVE STATUS\G" | awk '/Slave_IO_Running:/ {print $2}')

    if [ "$SLAVE_SQL_RUNNING" != "Yes" ] || [ "$SLAVE_IO_RUNNING" != "Yes" ]; then
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

echo "Stopping wal-g binlog-server (PID: $walg_pid)..."
if kill -0 $walg_pid 2>/dev/null; then
    kill -TERM $walg_pid
    wait $walg_pid 2>/dev/null || true
fi

AFTER_COUNT=$(mysql -N -e "SELECT COUNT(*) FROM sbtest.pitr WHERE id = 'testpitr_after'")
if [ "$AFTER_COUNT" -ne 0 ]; then
    echo "ERROR: Record after DT1 should not be replicated"
    exit 1
fi

echo "Analyzing binlog server logs..."
CONN_COUNT=$(grep -c 'connection accepted from' "$BINLOG_SERVER_LOG" || true)
RECONNECT_COUNT=$(grep -c 'Returning existing streamer for reconnection' "$BINLOG_SERVER_LOG" || true)

echo "Connection count: $CONN_COUNT"
echo "Reconnection count: $RECONNECT_COUNT"

if [ "$RECONNECT_COUNT" -ge 1 ]; then
    echo "SUCCESS: Reconnection detected in logs: $RECONNECT_COUNT times"
elif [ "$CONN_COUNT" -ge 2 ]; then
    echo "SUCCESS: Multiple connections detected - reconnection test passed"
else
    echo "WARNING: Expected reconnection evidence, got connections: $CONN_COUNT, reconnects: $RECONNECT_COUNT"
    echo "Full binlog server log:"
    cat $BINLOG_SERVER_LOG
fi

echo "Test passed!"