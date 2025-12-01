#!/bin/sh
set -e -x

. /usr/local/export_common.sh

s3cmd s3://mysql_pitr_binlogserver_reconnection_bucket || true
export WALE_S3_PREFIX=s3://mysql_pitr_binlogserver_reconnection_bucket
export WALG_MYSQL_BINLOG_SERVER_HOST="127.0.0.1"
export WALG_MYSQL_BINLOG_SERVER_PORT=9306
export WALG_MYSQL_BINLOG_SERVER_USER="walg"
export WALG_MYSQL_BINLOG_SERVER_PASSWORD="walgpwd"
export WALG_MYSQL_BINLOG_SERVER_ID=99
export WALG_MYSQL_BINLOG_SERVER_REPLICA_SOURCE="sbtest@tcp(127.0.0.1:3306)/sbtest"
export WALG_COMPRESSION_METHOD=zstd

PROXY_PORT=9307
BINLOG_SERVER_PORT=9306

SCRIPT_DIR="$(dirname "$0")"
PROXY_SCRIPT="$SCRIPT_DIR/binlog_proxy.py"

mysqld --initialize --init-file=/etc/mysql/init.sql
service mysql start

wal-g backup-push

mysql -e "CREATE TABLE sbtest.pitr(id VARCHAR(32), ts DATETIME)"
mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr01', NOW())"
mysql -e "FLUSH LOGS"
wal-g binlog-push

for i in $(seq 1 300); do
    mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr_batch_$i', NOW())"
    if [ $((i % 25)) -eq 0 ]; then
        mysql -e "FLUSH LOGS"
        sleep 1
        wal-g binlog-push
    fi
done

sleep 3
DT1=$(date3339)
sleep 3

mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr_after', NOW())"
mysql -e "FLUSH LOGS"
wal-g binlog-push

mysql_kill_and_clean_data
wal-g backup-fetch LATEST
chown -R mysql:mysql $MYSQLDATA
sleep 2
service mysql start || (cat /var/log/mysql/error.log && false)
mysql_set_gtid_purged

BINLOG_SERVER_LOG=/tmp/binlog_server_reconnect.log
PROXY_LOG=/tmp/proxy.log

check_port_listening() {
    local port=$1
    local host=${2:-127.0.0.1}

    if timeout 2 bash -c "echo >/dev/tcp/${host}/${port}" 2>/dev/null; then
        echo "Port $port is accepting connections"
        return 0
    fi

    return 1
}

safe_kill_process() {
    local pid=$1
    local name=$2

    if [ -z "$pid" ]; then
        echo "No PID provided for $name"
        return 0
    fi

    echo "Stopping $name (PID: $pid)..."

    if kill -0 "$pid" 2>/dev/null; then
        kill -TERM "$pid" 2>/dev/null || true
        sleep 2
        if kill -0 "$pid" 2>/dev/null; then
            echo "Force killing $name (PID: $pid)..."
            kill -9 "$pid" 2>/dev/null || true
        fi
        echo "$name stopped"
    else
        echo "$name was not running"
    fi
}

echo "Starting wal-g binlog-server..."
WALG_LOG_LEVEL="DEVEL" wal-g binlog-server --since LATEST --until "$DT1" 2>&1 | tee $BINLOG_SERVER_LOG &
walg_pid=$!
echo "Started wal-g binlog-server with PID: $walg_pid"

echo "Waiting for binlog-server to start..."
WAIT_COUNT=0
MAX_WAIT=10

while [ $WAIT_COUNT -lt $MAX_WAIT ]; do
    if ! kill -0 $walg_pid 2>/dev/null; then
        echo "ERROR: wal-g binlog-server process died"
        echo "=== Binlog server log ==="
        cat $BINLOG_SERVER_LOG
        exit 1
    fi

    if grep -q "Listening on.*wait connection" $BINLOG_SERVER_LOG 2>/dev/null; then
        echo "Binlog server reports it's listening"
        if check_port_listening $BINLOG_SERVER_PORT; then
            echo "Binlog server is ready and accepting connections"
            break
        else
            echo "Binlog server reports listening but port check failed, waiting..."
        fi
    fi
    sleep 2
    WAIT_COUNT=$((WAIT_COUNT + 1))
done

if [ $WAIT_COUNT -eq $MAX_WAIT ]; then
    echo "ERROR: Binlog server failed to start within $((MAX_WAIT * 2)) seconds"
    echo "=== Binlog server log ==="
    cat $BINLOG_SERVER_LOG
    exit 1
fi

sleep 5

echo "Starting proxy with max 2 reconnections..."
python3 "$PROXY_SCRIPT" $PROXY_PORT "127.0.0.1" $BINLOG_SERVER_PORT 2 > $PROXY_LOG 2>&1 & proxy_pid=$!
echo "Started proxy with PID: $proxy_pid"

echo "Waiting for proxy to start..."
WAIT_COUNT=0
while [ $WAIT_COUNT -lt 15 ]; do
    if ! kill -0 $proxy_pid 2>/dev/null; then
        echo "ERROR: Proxy process died"
        echo "=== Proxy log ==="
        cat $PROXY_LOG
        exit 1
    fi

    if check_port_listening $PROXY_PORT; then
        echo "Proxy is ready and accepting connections"
        break
    fi

    echo "Waiting for proxy... ($WAIT_COUNT/15)"
    sleep 1
    WAIT_COUNT=$((WAIT_COUNT + 1))
done

if [ $WAIT_COUNT -eq 15 ]; then
    echo "ERROR: Proxy failed to start within 15 seconds"
    echo "=== Proxy log ==="
    cat $PROXY_LOG
    exit 1
fi

echo "Configuring MySQL replication..."
mysql -e "STOP SLAVE"
mysql -e "RESET SLAVE ALL"
mysql -e "SET GLOBAL SERVER_ID = 123"
mysql -e "SET GLOBAL SLAVE_NET_TIMEOUT = 10"
mysql -e "CHANGE MASTER TO MASTER_HOST='127.0.0.1', MASTER_PORT=$PROXY_PORT, MASTER_USER='walg', MASTER_PASSWORD='walgpwd', MASTER_AUTO_POSITION=1, MASTER_CONNECT_RETRY=5, MASTER_RETRY_COUNT=86400"
mysql -e "START SLAVE"

echo "Waiting for replication to start..."
WAIT_COUNT=0
MAX_WAIT=60
while [ $WAIT_COUNT -lt $MAX_WAIT ]; do
    SLAVE_STATUS=$(mysql -e "SHOW SLAVE STATUS\G" 2>/dev/null || echo "")
    SLAVE_IO_RUNNING=$(echo "$SLAVE_STATUS" | grep "Slave_IO_Running:" | awk '{print $2}')
    SLAVE_SQL_RUNNING=$(echo "$SLAVE_STATUS" | grep "Slave_SQL_Running:" | awk '{print $2}')
    if [ "$SLAVE_IO_RUNNING" = "Yes" ]; then
        echo "Replication IO thread started successfully"
        break
    fi
    sleep 2
    WAIT_COUNT=$((WAIT_COUNT + 1))
done

echo "Waiting for replication to complete..."
MAX_WAIT=30
WAIT_COUNT=0
EXPECTED_ROWS=301
while [ $WAIT_COUNT -lt $MAX_WAIT ]; do
    ROW_COUNT=$(mysql -N -e "SELECT COUNT(*) FROM sbtest.pitr" 2>/dev/null || echo "0")
    SLAVE_IO_RUNNING=$(mysql -e "SHOW SLAVE STATUS\G" | grep "Slave_IO_Running:" | awk '{print $2}')
    SLAVE_SQL_RUNNING=$(mysql -e "SHOW SLAVE STATUS\G" | grep "Slave_SQL_Running:" | awk '{print $2}')

    LAST_IO_ERROR=$(mysql -e "SHOW SLAVE STATUS\G" | grep "Last_IO_Error:" | cut -d: -f2-)
    LAST_SQL_ERROR=$(mysql -e "SHOW SLAVE STATUS\G" | grep "Last_SQL_Error:" | cut -d: -f2-)

    mysql -e "SHOW SLAVE STATUS\G" | grep -E "(Retrieved_Gtid_Set|Executed_Gtid_Set)"
    echo "Row count: $ROW_COUNT / $EXPECTED_ROWS, IO: $SLAVE_IO_RUNNING, SQL: $SLAVE_SQL_RUNNING (wait: $WAIT_COUNT/$MAX_WAIT)"

    if [ -n "$LAST_IO_ERROR" ] && [ "$LAST_IO_ERROR" != " " ]; then
        echo "Last IO Error: $LAST_IO_ERROR"
    fi
    if [ -n "$LAST_SQL_ERROR" ] && [ "$LAST_SQL_ERROR" != " " ]; then
        echo "Last SQL Error: $LAST_SQL_ERROR"
    fi

    if ! kill -0 $walg_pid 2>/dev/null; then
        echo "WARNING: wal-g binlog-server process died!"
        echo "=== Last lines of binlog server log ==="
        tail -20 $BINLOG_SERVER_LOG
        break
    fi

    if ! kill -0 $proxy_pid 2>/dev/null; then
        echo "WARNING: Proxy process died!"
        echo "=== Last lines of proxy log ==="
        tail -20 $PROXY_LOG
        break
    fi

    if [ "$ROW_COUNT" -ge "$EXPECTED_ROWS" ]; then
        echo "Replication completed successfully"
        break
    fi

    sleep 2
    WAIT_COUNT=$((WAIT_COUNT + 1))
done

safe_kill_process "$proxy_pid" "proxy"

FINAL_ROW_COUNT=$(mysql -N -e "SELECT COUNT(*) FROM sbtest.pitr")
AFTER_COUNT=$(mysql -N -e "SELECT COUNT(*) FROM sbtest.pitr WHERE id = 'testpitr_after'")

if [ "$AFTER_COUNT" -ne 0 ]; then
    echo "ERROR: Record after DT1 should not be replicated"
    exit 1
fi

PROXY_RECONNECTS=$(grep -c "Planned disconnect" "$PROXY_LOG" 2>/dev/null || echo "0")

if [ "$FINAL_ROW_COUNT" -ge "$EXPECTED_ROWS" ]; then
    echo "- Data replicated successfully: $FINAL_ROW_COUNT rows"
    echo "- Network disconnects: $PROXY_RECONNECTS (limited to 2)"
else
    echo "ERROR: Test failed"
    echo "- Expected $EXPECTED_ROWS rows, got $FINAL_ROW_COUNT"
    echo "- Proxy reconnects: $PROXY_RECONNECTS"
    exit 1
fi

echo "Test completed successfully!"