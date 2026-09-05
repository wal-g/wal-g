#!/bin/sh
set -e -x

# shellcheck disable=SC1091
. /usr/local/export_common.sh

log() {
    echo "$(date '+%Y/%m/%d %H:%M:%S.%N' | cut -b1-26) $*"
}

s3cmd s3://mysql-pitr-binlogserver-reconnection-bucket || true
export WALE_S3_PREFIX=s3://mysql-pitr-binlogserver-reconnection-bucket
export WALG_MYSQL_BINLOG_SERVER_HOST="127.0.0.1"
export WALG_MYSQL_BINLOG_SERVER_PORT=9306
export WALG_MYSQL_BINLOG_SERVER_USER="walg"
export WALG_MYSQL_BINLOG_SERVER_PASSWORD="walgpwd"
export WALG_MYSQL_BINLOG_SERVER_ID=99
export WALG_MYSQL_BINLOG_SERVER_REPLICA_SOURCE="sbtest@tcp(127.0.0.1:3306)/sbtest"
export WALG_COMPRESSION_METHOD=zstd

PROXY_PORT=9307
BINLOG_SERVER_PORT=9306
PLANNED_DISCONNECTS=3

SCRIPT_DIR="$(dirname "$0")"
PROXY_SCRIPT="$SCRIPT_DIR/../utils/binlog_proxy.py"

mysqld --initialize --init-file=/etc/mysql/init.sql
service mysql start

mysql -e "SELECT UNIX_TIMESTAMP();"
mysql -e "SELECT @@GLOBAL.SERVER_UUID;"
mysql -e "SELECT @@global.binlog_checksum; SET @master_binlog_checksum:=@@global.binlog_checksum; SELECT @master_binlog_checksum;"

wal-g backup-push

mysql -e "CREATE TABLE sbtest.pitr(id VARCHAR(32), ts DATETIME)"
mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr01', NOW())"
mysql -e "FLUSH LOGS"
wal-g binlog-push

for i in $(seq 1 360); do
    mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr_batch_$i', NOW())"
    if [ $((i % 20)) -eq 0 ]; then
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
chown -R mysql:mysql "$MYSQLDATA"
sleep 2
service mysql start || (cat /var/log/mysql/error.log && false)
mysql_set_gtid_purged

BINLOG_SERVER_LOG=/tmp/binlog_server_reconnect.log
PROXY_LOG=/tmp/proxy.log
walg_pid=""
proxy_pid=""

check_port_listening() {
    check_port_listening_port="$1"
    check_port_listening_host="${2:-127.0.0.1}"

    if timeout 2 bash -c "echo >/dev/tcp/${check_port_listening_host}/${check_port_listening_port}" 2>/dev/null; then
        log "Port $check_port_listening_port is accepting connections"
        return 0
    fi

    return 1
}

safe_kill_process() {
    safe_kill_process_pid="$1"
    safe_kill_process_name="$2"

    if [ -z "$safe_kill_process_pid" ]; then
        log "No PID provided for $safe_kill_process_name"
        return 0
    fi

    log "Stopping $safe_kill_process_name (PID: $safe_kill_process_pid)..."

    if kill -0 "$safe_kill_process_pid" 2>/dev/null; then
        kill -TERM "$safe_kill_process_pid" 2>/dev/null || true
        sleep 2
        if kill -0 "$safe_kill_process_pid" 2>/dev/null; then
            log "Force killing $safe_kill_process_name (PID: $safe_kill_process_pid)..."
            kill -9 "$safe_kill_process_pid" 2>/dev/null || true
        fi
        log "$safe_kill_process_name stopped"
    else
        log "$safe_kill_process_name was not running"
    fi

    wait "$safe_kill_process_pid" 2>/dev/null || true
}

cleanup() {
    cleanup_status=$?

    mysql_stop_replica >/dev/null 2>&1 || true

    if [ -n "$proxy_pid" ]; then
        safe_kill_process "$proxy_pid" "proxy"
        proxy_pid=""
    fi

    if [ -n "$walg_pid" ]; then
        safe_kill_process "$walg_pid" "wal-g binlog-server"
        walg_pid=""
    fi

    if [ "$cleanup_status" -ne 0 ]; then
        log "=== Binlog server log ==="
        [ ! -f "$BINLOG_SERVER_LOG" ] || cat "$BINLOG_SERVER_LOG"
        log "=== Proxy log ==="
        [ ! -f "$PROXY_LOG" ] || cat "$PROXY_LOG"
    fi

    return "$cleanup_status"
}

trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

log "Starting wal-g binlog-server..."
WALG_LOG_LEVEL="DEVEL" wal-g binlog-server --since LATEST --until "$DT1" >"$BINLOG_SERVER_LOG" 2>&1 &
walg_pid=$!
log "Started wal-g binlog-server with PID: $walg_pid"

log "Waiting for binlog-server to start..."
WAIT_COUNT=0
MAX_WAIT=30

while [ "$WAIT_COUNT" -lt "$MAX_WAIT" ]; do
    if ! kill -0 "$walg_pid" 2>/dev/null; then
        log "ERROR: wal-g binlog-server process died"
        log "=== Binlog server log ==="
        cat "$BINLOG_SERVER_LOG"
        exit 1
    fi

    if grep -q "Listening on" "$BINLOG_SERVER_LOG" 2>/dev/null; then
        log "Binlog server reports it's listening"
        if check_port_listening "$BINLOG_SERVER_PORT"; then
            log "Binlog server is ready and accepting connections"
            break
        else
            log "Binlog server reports listening but port check failed, waiting..."
        fi
    fi
    sleep 2
    WAIT_COUNT=$((WAIT_COUNT + 1))
done

if [ "$WAIT_COUNT" -eq "$MAX_WAIT" ]; then
    log "ERROR: Binlog server failed to start within $((MAX_WAIT * 2)) seconds"
    log "=== Binlog server log ==="
    cat "$BINLOG_SERVER_LOG"
    exit 1
fi

sleep 3

log "Starting proxy with reconnections..."
python3 "$PROXY_SCRIPT" "$PROXY_PORT" "127.0.0.1" "$BINLOG_SERVER_PORT" "$PLANNED_DISCONNECTS" >"$PROXY_LOG" 2>&1 &
proxy_pid=$!
log "Started proxy with PID: $proxy_pid"

log "Waiting for proxy to start..."
sleep 7
if ! kill -0 "$proxy_pid" 2>/dev/null; then
    log "ERROR: Proxy process died"
    cat "$PROXY_LOG"
    exit 1
fi
log "Proxy should be ready"


log "Configuring MySQL replication..."
mysql_stop_replica
mysql_reset_replica_all
mysql -e "SET GLOBAL SERVER_ID = 123"
mysql_set_replica_net_timeout 10
mysql_set_replica_transaction_retries 20
mysql -e "SET GLOBAL log_error_verbosity=3; SET GLOBAL general_log=1;"
mysql_change_replication_source "127.0.0.1" "$PROXY_PORT" "walg" "walgpwd" 1 2 86400
mysql_start_replica

log "Waiting for replication to start..."
WAIT_COUNT=0
MAX_WAIT=30
LAST_ROW_COUNT=-1
STUCK_COUNTER=0
while [ "$WAIT_COUNT" -lt "$MAX_WAIT" ]; do
    SLAVE_STATUS=$(mysql_show_replica_status 2>/dev/null || echo "")
    SLAVE_IO_RUNNING=$(echo "$SLAVE_STATUS" | grep -E "(Replica|Slave)_IO_Running:" | awk '{print $2}')

    if [ "$SLAVE_IO_RUNNING" = "Yes" ]; then
        log "Replication IO thread started successfully"
        break
    fi
    sleep 2
    WAIT_COUNT=$((WAIT_COUNT + 1))
done

log "Waiting for replication to complete..."

MAX_WAIT=30
WAIT_COUNT=0
EXPECTED_ROWS=361
while [ "$WAIT_COUNT" -lt "$MAX_WAIT" ]; do
    ROW_COUNT=$(mysql -N -e "SELECT COUNT(*) FROM sbtest.pitr" 2>/dev/null || echo "0")
    SLAVE_STATUS=$(mysql_show_replica_status)
    SLAVE_IO_RUNNING=$(echo "$SLAVE_STATUS" | grep -E "(Replica|Slave)_IO_Running:" | awk '{print $2}')
    SLAVE_SQL_RUNNING=$(echo "$SLAVE_STATUS" | grep -E "(Replica|Slave)_SQL_Running:" | awk '{print $2}')

    LAST_IO_ERROR=$(echo "$SLAVE_STATUS" | grep "Last_IO_Error:" | cut -d: -f2-)

    if [ "$ROW_COUNT" -eq "$LAST_ROW_COUNT" ]; then
        STUCK_COUNTER=$((STUCK_COUNTER + 1))
    else
        STUCK_COUNTER=0
        LAST_ROW_COUNT=$ROW_COUNT
    fi

    if [ "$SLAVE_IO_RUNNING" = "No" ] && [ -n "$LAST_IO_ERROR" ]; then
        log "Slave IO failed, attempting quick restart..."
        mysql_stop_replica
        mysql_start_replica
        sleep 2
    fi


    log "Row count: $ROW_COUNT / $EXPECTED_ROWS, IO: $SLAVE_IO_RUNNING, SQL: $SLAVE_SQL_RUNNING (wait: $WAIT_COUNT/$MAX_WAIT)"

    if ! kill -0 "$walg_pid" 2>/dev/null; then
        log "WARNING: wal-g binlog-server process died!"
        break
    fi

    if [ "$ROW_COUNT" -ge "$EXPECTED_ROWS" ]; then
        log "Replication completed successfully"
        break
    fi

    sleep 2
    WAIT_COUNT=$((WAIT_COUNT + 1))
done

cat "$PROXY_LOG"

cleanup

if check_port_listening "$PROXY_PORT"; then
    log "ERROR: Proxy port $PROXY_PORT is still accepting connections after cleanup"
    exit 1
fi

if check_port_listening "$BINLOG_SERVER_PORT"; then
    log "ERROR: Binlog server port $BINLOG_SERVER_PORT is still accepting connections after cleanup"
    exit 1
fi

log "MYSQL ERRORS:"
cat /var/log/mysql/error.log


FINAL_ROW_COUNT=$(mysql -N -e "SELECT COUNT(*) FROM sbtest.pitr")
AFTER_COUNT=$(mysql -N -e "SELECT COUNT(*) FROM sbtest.pitr WHERE id = 'testpitr_after'")

if [ "$AFTER_COUNT" -ne 0 ]; then
    log "ERROR: Record after DT1 should not be replicated"
    exit 1
fi

PROXY_RECONNECTS=$(grep -c "Disconnect #" "$PROXY_LOG" 2>/dev/null || echo "0")

if [ "$FINAL_ROW_COUNT" -ge "$EXPECTED_ROWS" ]; then
    log "- Data replicated successfully: $FINAL_ROW_COUNT rows"
    log "- Network disconnects: $PROXY_RECONNECTS (planned: $PLANNED_DISCONNECTS)"
else
    log "ERROR: Test failed"
    log "- Expected $EXPECTED_ROWS rows, got $FINAL_ROW_COUNT"
    log "- Proxy reconnects: $PROXY_RECONNECTS"
    exit 1
fi


log "Test completed successfully!"
