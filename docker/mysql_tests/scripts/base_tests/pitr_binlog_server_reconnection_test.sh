#!/bin/sh
set -e -x

. /usr/local/export_common.sh

log() {
    echo "$(date '+%Y/%m/%d %H:%M:%S.%N' | cut -b1-26) $*"
}

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
PLANNED_DISCONNECTS=3

SCRIPT_DIR="$(dirname "$0")"
PROXY_SCRIPT="$SCRIPT_DIR/../utils/binlog_proxy.py"

mysqld --initialize --init-file=/etc/mysql/init.sql
service mysql start

mysql -e "SELECT UNIX_TIMESTAMP();"
mysql -e "SELECT @@GLOBAL.SERVER_UUID;"
mysql -e "SELECT @@global.binlog_checksum; SET @master_binlog_checksum:=@@global.binlog_checksum; SELECT @master_binlog_checksum;"

mysql -e "CREATE TABLE sbtest.pitr(id VARCHAR(32), ts DATETIME)"
wal-g backup-push

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
        log "Port $port is accepting connections"
        return 0
    fi

    return 1
}

safe_kill_process() {
    local pid=$1
    local name=$2

    if [ -z "$pid" ]; then
        log "No PID provided for $name"
        return 0
    fi

    log "Stopping $name (PID: $pid)..."

    if kill -0 "$pid" 2>/dev/null; then
        kill -TERM "$pid" 2>/dev/null || true
        sleep 2
        if kill -0 "$pid" 2>/dev/null; then
            log "Force killing $name (PID: $pid)..."
            kill -9 "$pid" 2>/dev/null || true
        fi
        log "$name stopped"
    else
        log "$name was not running"
    fi
}

log "Starting wal-g binlog-server..."
WALG_LOG_LEVEL="DEVEL" wal-g binlog-server --since LATEST --until "$DT1" 2>&1 | tee $BINLOG_SERVER_LOG &
walg_pid=$!
log "Started wal-g binlog-server with PID: $walg_pid"

log "Waiting for binlog-server to start..."
WAIT_COUNT=0
MAX_WAIT=20

while [ $WAIT_COUNT -lt $MAX_WAIT ]; do
    if ! kill -0 $walg_pid 2>/dev/null; then
        log "ERROR: wal-g binlog-server process died"
        log "=== Binlog server log ==="
        cat $BINLOG_SERVER_LOG
        exit 1
    fi

    if grep -q "Listening on" $BINLOG_SERVER_LOG 2>/dev/null; then
        log "Binlog server reports it's listening"
        if check_port_listening $BINLOG_SERVER_PORT; then
            log "Binlog server is ready and accepting connections"
            break
        else
            log "Binlog server reports listening but port check failed, waiting..."
        fi
    fi
    sleep 2
    WAIT_COUNT=$((WAIT_COUNT + 1))
done

if [ $WAIT_COUNT -eq $MAX_WAIT ]; then
    log "ERROR: Binlog server failed to start within $((MAX_WAIT * 2)) seconds"
    log "=== Binlog server log ==="
    cat $BINLOG_SERVER_LOG
    exit 1
fi

sleep 5

log "Starting proxy with reconnections..."
python3 "$PROXY_SCRIPT" $PROXY_PORT "127.0.0.1" $BINLOG_SERVER_PORT $PLANNED_DISCONNECTS > $PROXY_LOG 2>&1 & proxy_pid=$!
log "Started proxy with PID: $proxy_pid"

log "Waiting for proxy to start..."
sleep 15
if ! kill -0 $proxy_pid 2>/dev/null; then
    log "ERROR: Proxy process died"
    cat $PROXY_LOG
    exit 1
fi
log "Proxy should be ready"


log "Configuring MySQL replication..."
mysql -e "STOP SLAVE"
mysql -e "RESET SLAVE ALL"
mysql -e "SET GLOBAL SERVER_ID = 123"
mysql -e "SET GLOBAL SLAVE_NET_TIMEOUT = 10"
mysql -e "SET GLOBAL slave_transaction_retries = 20"
mysql -e "SET GLOBAL log_error_verbosity=3; SET GLOBAL general_log=1;"
mysql -e "CHANGE MASTER TO MASTER_HOST='127.0.0.1', MASTER_PORT=$PROXY_PORT, MASTER_USER='walg', MASTER_PASSWORD='walgpwd', MASTER_AUTO_POSITION=1, MASTER_CONNECT_RETRY=1, MASTER_HEARTBEAT_PERIOD=2, MASTER_RETRY_COUNT=86400"
mysql -e "START SLAVE"

log "Waiting for replication to start..."
WAIT_COUNT=0
MAX_WAIT=15
LAST_ROW_COUNT=-1
STUCK_COUNTER=0
STUCK_THRESHOLD=5
while [ $WAIT_COUNT -lt $MAX_WAIT ]; do
    SLAVE_STATUS=$(mysql -e "SHOW SLAVE STATUS\G" 2>/dev/null || echo "")
    SLAVE_IO_RUNNING=$(echo "$SLAVE_STATUS" | grep "Slave_IO_Running:" | awk '{print $2}')

    if [ "$SLAVE_IO_RUNNING" = "Yes" ]; then
        log "Replication IO thread started successfully"
        break
    fi
    sleep 2
    WAIT_COUNT=$((WAIT_COUNT + 1))
done

log "Waiting for replication to complete..."

MAX_WAIT=15
WAIT_COUNT=0
EXPECTED_ROWS=301
while [ $WAIT_COUNT -lt $MAX_WAIT ]; do
    ROW_COUNT=$(mysql -N -e "SELECT COUNT(*) FROM sbtest.pitr" 2>/dev/null || echo "0")
    SLAVE_IO_RUNNING=$(mysql -e "SHOW SLAVE STATUS\G" | grep "Slave_IO_Running:" | awk '{print $2}')
    SLAVE_SQL_RUNNING=$(mysql -e "SHOW SLAVE STATUS\G" | grep "Slave_SQL_Running:" | awk '{print $2}')

    LAST_IO_ERROR=$(mysql -e "SHOW SLAVE STATUS\G" | grep "Last_IO_Error:" | cut -d: -f2-)
    LAST_SQL_ERROR=$(mysql -e "SHOW SLAVE STATUS\G" | grep "Last_SQL_Error:" | cut -d: -f2-)

    if [ "$ROW_COUNT" -eq "$LAST_ROW_COUNT" ]; then
        STUCK_COUNTER=$((STUCK_COUNTER + 1))
    else
        STUCK_COUNTER=0
        LAST_ROW_COUNT=$ROW_COUNT
    fi

    if [ "$SLAVE_IO_RUNNING" = "No" ] && [ -n "$LAST_IO_ERROR" ]; then
        log "Slave IO failed, attempting quick restart..."
        mysql -e "STOP SLAVE; START SLAVE;"
        sleep 2
    fi

#    if ([ "$SLAVE_IO_RUNNING" = "No" ] && [ -n "$LAST_IO_ERROR" ]) || [ $STUCK_COUNTER -ge $STUCK_THRESHOLD ]; then
#        log "Replication stuck or failed (IO: $SLAVE_IO_RUNNING, Stuck: $STUCK_COUNTER). Kicking slave..."
#        mysql -e "STOP SLAVE; START SLAVE;"
#        STUCK_COUNTER=0
#        sleep 2
#    fi

    log "Row count: $ROW_COUNT / $EXPECTED_ROWS, IO: $SLAVE_IO_RUNNING, SQL: $SLAVE_SQL_RUNNING (wait: $WAIT_COUNT/$MAX_WAIT)"

    if ! kill -0 $walg_pid 2>/dev/null; then
        log "WARNING: wal-g binlog-server process died!"
        # cat $BINLOG_SERVER_LOG
        break
    fi

    if [ "$ROW_COUNT" -ge "$EXPECTED_ROWS" ]; then
        log "Replication completed successfully"
        break
    fi

    sleep 2
    WAIT_COUNT=$((WAIT_COUNT + 1))
done

# cat $BINLOG_SERVER_LOG
cat $PROXY_LOG

safe_kill_process "$proxy_pid" "proxy"

FINAL_ROW_COUNT=$(mysql -N -e "SELECT COUNT(*) FROM sbtest.pitr")
AFTER_COUNT=$(mysql -N -e "SELECT COUNT(*) FROM sbtest.pitr WHERE id = 'testpitr_after'")

if [ "$AFTER_COUNT" -ne 0 ]; then
    log "ERROR: Record after DT1 should not be replicated"
    exit 1
fi
ls /var/log/
ls /var/log/mysql
log "MYSQL ERRORS!!!"
cat /var/log/mysql/error.log
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