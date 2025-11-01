#!/bin/sh
set -e -x

. /usr/local/export_common.sh

s3cmd mb s3://mysql-pitr-binlogserver-reconnection-bucket || true
export WALE_S3_PREFIX=s3://mysql-pitr-binlogserver-reconnection-bucket
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

for i in $(seq 1 1000); do
    mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr_batch_$i', NOW())"
    if [ $((i % 50)) -eq 0 ]; then
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

sleep 4

SLAVE_IO_RUNNING=$(mysql -e "SHOW SLAVE STATUS\G" | grep "Slave_IO_Running: Yes" | wc -l)
if [ "$SLAVE_IO_RUNNING" -eq 1 ]; then
    echo "Replication started successfully"
else
    echo "ERROR: Replication IO thread did not start initially"
    mysql -e "SHOW SLAVE STATUS\G"
    exit 1
fi

sleep 4
CURRENT_COUNT=$(mysql -N -e "SELECT COUNT(*) FROM sbtest.pitr")
echo "Current row count during replication: $CURRENT_COUNT"

echo "Simulating network connection loss during replication..."

BINLOG_SERVER_PORT=9306

echo "Killing TCP connections to binlog server using ss..."
ss -K dport = $BINLOG_SERVER_PORT 2>/dev/null || true

echo "TCP connections killed, waiting for reconnection..."
sleep 5

SLAVE_IO_STATE=$(mysql -e "SHOW SLAVE STATUS\G" | grep "Slave_IO_State:" | head -1)
echo "Slave IO State after connection kill: $SLAVE_IO_STATE"

sleep 15

SLAVE_IO_RUNNING=$(mysql -e "SHOW SLAVE STATUS\G" | grep "Slave_IO_Running: Yes" | wc -l)
if [ "$SLAVE_IO_RUNNING" -eq 1 ]; then
    echo "Replication restored successfully after network reconnect"
else
    echo "Checking if replication is still in progress..."
    SLAVE_IO_STATE=$(mysql -e "SHOW SLAVE STATUS\G" | grep "Slave_IO_State:" | head -1)
    echo "Current Slave IO State: $SLAVE_IO_STATE"

    if echo "$SLAVE_IO_STATE" | grep -q "onnect"; then
        echo "Replication is reconnecting, waiting more..."
        sleep 10
        SLAVE_IO_RUNNING=$(mysql -e "SHOW SLAVE STATUS\G" | grep "Slave_IO_Running: Yes" | wc -l)
        if [ "$SLAVE_IO_RUNNING" -eq 1 ]; then
            echo "Replication restored after additional wait"
        else
            echo "ERROR: Replication IO thread did not restore after reconnect"
            mysql -e "SHOW SLAVE STATUS\G"
            exit 1
        fi
    else
        echo "ERROR: Replication IO thread did not restore after reconnect"
        mysql -e "SHOW SLAVE STATUS\G"
        exit 1
    fi
fi

# Ждем пока реплика догонит все данные
echo "Waiting for replication to complete..."
MAX_WAIT=60
WAIT_COUNT=0
while [ $WAIT_COUNT -lt $MAX_WAIT ]; do
    ROW_COUNT=$(mysql -N -e "SELECT COUNT(*) FROM sbtest.pitr")
    echo "Current row count: $ROW_COUNT / 1001"

    if [ "$ROW_COUNT" -eq 1001 ]; then
        echo "Replication completed successfully"
        break
    fi

    SLAVE_SQL_RUNNING=$(mysql -e "SHOW SLAVE STATUS\G" | grep "Slave_SQL_Running: Yes" | wc -l)
    SLAVE_IO_RUNNING=$(mysql -e "SHOW SLAVE STATUS\G" | grep "Slave_IO_Running: Yes" | wc -l)

    if [ "$SLAVE_SQL_RUNNING" -eq 0 ] || [ "$SLAVE_IO_RUNNING" -eq 0 ]; then
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

# Теперь можем останавливать wal-g
echo "Stopping wal-g binlog-server (PID: $walg_pid)..."
if kill -0 $walg_pid 2>/dev/null; then
    echo "Sending SIGTERM to wal-g..."
    kill -TERM $walg_pid 2>/dev/null || true

    # Ждем до 30 секунд пока процесс завершится
    TIMEOUT=30
    ELAPSED=0
    while [ $ELAPSED -lt $TIMEOUT ]; do
        if ! kill -0 $walg_pid 2>/dev/null; then
            echo "wal-g stopped successfully after ${ELAPSED}s"
            break
        fi
        sleep 1
        ELAPSED=$((ELAPSED + 1))
    done

    # Если все еще работает, убиваем силой
    if kill -0 $walg_pid 2>/dev/null; then
        echo "wal-g did not stop gracefully, sending SIGKILL..."
        kill -9 $walg_pid 2>/dev/null || true
        sleep 1
    fi
else
    echo "wal-g process already exited"
fi

# Проверяем код выхода (если процесс еще можно дождаться)
wait $walg_pid 2>/dev/null && echo "wal-g exit code: $?" || echo "wal-g already terminated"

ROW_COUNT=$(mysql -N -e "SELECT COUNT(*) FROM sbtest.pitr")
EXPECTED_COUNT=1001

if [ "$ROW_COUNT" -ne "$EXPECTED_COUNT" ]; then
    echo "ERROR: Expected $EXPECTED_COUNT rows, got $ROW_COUNT"
    exit 1
fi

AFTER_COUNT=$(mysql -N -e "SELECT COUNT(*) FROM sbtest.pitr WHERE id = 'testpitr_after'")
if [ "$AFTER_COUNT" -ne 0 ]; then
    echo "ERROR: Record after DT1 should not be replicated"
    exit 1
fi

CONN_COUNT=$(grep -c 'connection accepted from' "$BINLOG_SERVER_LOG" || true)
echo "Total connections detected: $CONN_COUNT"

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