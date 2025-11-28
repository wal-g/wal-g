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

cat > /tmp/binlog_proxy.py << 'EOF'
#!/usr/bin/env python3
import socket
import threading
import time
import sys
import select

class TwoDisconnectBinlogProxy:
    def __init__(self, listen_port, target_host, target_port, planned_disconnects=5):
        self.listen_port = listen_port
        self.target_host = target_host
        self.target_port = target_port
        self.running = True
        self.client_socket = None
        self.server_socket = None
        self.disconnect_count = 0
        self.planned_disconnects = planned_disconnects
        self.bytes_transferred = 0
        self.connection_start_time = None
        self.total_bytes_transferred = 0
        self.disconnects_completed = False

    def connect_to_server(self):
        try:
            if self.server_socket:
                self.server_socket.close()

            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.settimeout(10)
            self.server_socket.connect((self.target_host, self.target_port))
            print(f"[Proxy] Connected to binlog server (disconnect #{self.disconnect_count})")
            return True
        except Exception as e:
            print(f"[Proxy] Failed to connect to binlog server: {e}")
            return False

    def should_disconnect(self):
        if self.disconnects_completed:
            return False

        if not self.connection_start_time:
            return False

        if self.bytes_transferred > 350 and self.disconnect_count < self.planned_disconnects:
            return True

        return False

    def handle_client_connection(self, client_socket):
        self.client_socket = client_socket
        print(f"[Proxy] Client connected from {client_socket.getpeername()}")

        if not self.connect_to_server():
            print("[Proxy] Initial connection to server failed")
            return

        self.connection_start_time = time.time()
        self.bytes_transferred = 0

        try:
            while self.running:
                if self.should_disconnect():
                    print(f"[Proxy] Planned disconnect #{self.disconnect_count + 1}/{self.planned_disconnects} after {self.bytes_transferred} bytes")
                    print(f"[Proxy] Total bytes transferred so far: {self.total_bytes_transferred}")

                    self.server_socket.close()
                    time.sleep(2)

                    self.disconnect_count += 1

                    if self.disconnect_count >= self.planned_disconnects:
                        self.disconnects_completed = True
                        print(f"[Proxy] Completed all {self.planned_disconnects} planned disconnects. Now working in stable mode.")

                    if not self.connect_to_server():
                        print("[Proxy] Reconnection failed, closing client connection")
                        break

                    self.connection_start_time = time.time()
                    self.bytes_transferred = 0

                try:
                    ready_sockets, _, error_sockets = select.select(
                        [self.client_socket, self.server_socket], [],
                        [self.client_socket, self.server_socket], 1.0
                    )

                    if error_sockets:
                        print("[Proxy] Socket error detected")
                        break

                    if not ready_sockets:
                        continue

                    if self.client_socket in ready_sockets:
                        try:
                            data = self.client_socket.recv(8192)
                            if not data:
                                print("[Proxy] Client disconnected")
                                break
                            self.server_socket.send(data)
                            self.bytes_transferred += len(data)
                            self.total_bytes_transferred += len(data)

                            mode = "STABLE" if self.disconnects_completed else f"DISCONNECT_MODE({self.disconnect_count}/{self.planned_disconnects})"
                            print(f"[Proxy] [{mode}] Client->Server: {len(data)} bytes (session: {self.bytes_transferred}, total: {self.total_bytes_transferred})")
                        except Exception as e:
                            print(f"[Proxy] Error forwarding client->server: {e}")
                            break

                    if self.server_socket in ready_sockets:
                        try:
                            data = self.server_socket.recv(8192)
                            if not data:
                                print("[Proxy] Server disconnected")
                                break
                            self.client_socket.send(data)
                            self.bytes_transferred += len(data)
                            self.total_bytes_transferred += len(data)

                            mode = "STABLE" if self.disconnects_completed else f"DISCONNECT_MODE({self.disconnect_count}/{self.planned_disconnects})"
                            print(f"[Proxy] [{mode}] Server->Client: {len(data)} bytes (session: {self.bytes_transferred}, total: {self.total_bytes_transferred})")
                        except Exception as e:
                            print(f"[Proxy] Error forwarding server->client: {e}")
                            break

                except Exception as e:
                    print(f"[Proxy] Select error: {e}")
                    break

        except Exception as e:
            print(f"[Proxy] Connection handling error: {e}")
        finally:
            final_mode = "STABLE" if self.disconnects_completed else "INCOMPLETE"
            print(f"[Proxy] Connection closed in {final_mode} mode")
            print(f"[Proxy] Total disconnects: {self.disconnect_count}/{self.planned_disconnects}")
            print(f"[Proxy] Total bytes transferred: {self.total_bytes_transferred}")
            if self.client_socket:
                self.client_socket.close()
            if self.server_socket:
                self.server_socket.close()

    def start(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind(('127.0.0.1', self.listen_port))
        server_socket.listen(1)

        print(f"[Proxy] Listening on port {self.listen_port}")
        print(f"[Proxy] Will make {self.planned_disconnects} planned disconnects, then work stably")

        try:
            while self.running:
                try:
                    client_socket, addr = server_socket.accept()
                    self.handle_client_connection(client_socket)
                except Exception as e:
                    if self.running:
                        print(f"[Proxy] Accept error: {e}")
        finally:
            server_socket.close()
            print("[Proxy] Server socket closed")

if __name__ == "__main__":
    proxy = TwoDisconnectBinlogProxy(9307, "127.0.0.1", 9306, planned_disconnects=2)
    proxy.start()
EOF

chmod +x /tmp/binlog_proxy.py

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
python3 /tmp/binlog_proxy.py > $PROXY_LOG 2>&1 & proxy_pid=$!
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
# cat $PROXY_LOG
PROXY_RECONNECTS=$(grep -c "Planned disconnect" "$PROXY_LOG" 2>/dev/null || echo "0")
BINLOG_CONNECTIONS=$(grep -c 'connection accepted from' "$BINLOG_SERVER_LOG" 2>/dev/null || echo "0")

echo "Proxy reconnects: $PROXY_RECONNECTS (expected: 2)"
echo "Binlog server connections: $BINLOG_CONNECTIONS"


if [ "$FINAL_ROW_COUNT" -ge "$EXPECTED_ROWS" ]; then
    echo "- Data replicated successfully: $FINAL_ROW_COUNT rows"
    echo "- Network disconnects: $PROXY_RECONNECTS (limited to 2)"
else
    echo "ERROR: Test failed"
    echo "- Expected $EXPECTED_ROWS rows, got $FINAL_ROW_COUNT"
    echo "- Proxy reconnects: $PROXY_RECONNECTS (expected: 2)"
    exit 1
fi

echo "Test completed successfully!"