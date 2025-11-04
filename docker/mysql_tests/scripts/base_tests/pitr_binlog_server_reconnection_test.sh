#!/bin/sh
set -e -x

. /usr/local/export_common.sh

# Проверка и установка python3
if ! command -v python3 >/dev/null 2>&1; then
    echo "python3 not found, installing..."
    apt-get update
    apt-get install -y python3
fi

# Проверка и установка awk при необходимости
if ! command -v awk >/dev/null 2>&1; then
    echo "awk not found, installing..."
    apt-get update
    apt-get install -y gawk
fi

s3cmd s3://mysql_pitr_binlogserver_reconnection_bucket || true
export WALE_S3_PREFIX=s3://mysql-pitr-binlogserver-reconnection-bucket
export WALG_MYSQL_BINLOG_SERVER_HOST="localhost"
export WALG_MYSQL_BINLOG_SERVER_PORT=9306
export WALG_MYSQL_BINLOG_SERVER_USER="walg"
export WALG_MYSQL_BINLOG_SERVER_PASSWORD="walgpwd"
export WALG_MYSQL_BINLOG_SERVER_ID=99
export WALG_MYSQL_BINLOG_SERVER_REPLICA_SOURCE="sbtest@tcp(127.0.0.1:3306)/sbtest"

# Порты для прокси
PROXY_PORT=9307
BINLOG_SERVER_PORT=9306

mysqld --initialize --init-file=/etc/mysql/init.sql
service mysql start

wal-g backup-push

mysql -e "CREATE TABLE sbtest.pitr(id VARCHAR(32), ts DATETIME)"
mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr01', NOW())"
mysql -e "FLUSH LOGS"
wal-g binlog-push

# Генерируем достаточно данных для нескольких разрывов соединения
for i in $(seq 1 2000); do
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
PROXY_LOG=/tmp/proxy.log

# Запуск wal-g binlog-server
WALG_LOG_LEVEL="DEVEL" wal-g binlog-server --since LATEST --until "$DT1" 2>&1 | tee $BINLOG_SERVER_LOG &
walg_pid=$!
echo "Started wal-g binlog-server with PID: $walg_pid"
sleep 3

# Копируем прокси скрипт
cat > /tmp/binlog_proxy.py << 'EOF'
#!/usr/bin/env python3
import socket
import threading
import time
import sys

class BinlogProxy:
    def __init__(self, listen_port, target_host, target_port, disconnect_after_bytes=10*1024*1024):
        self.listen_port = listen_port
        self.target_host = target_host
        self.target_port = target_port
        self.disconnect_after_bytes = disconnect_after_bytes
        self.bytes_transferred = 0
        self.connection_count = 0
        self.running = True

    def handle_client(self, client_socket):
        self.connection_count += 1
        connection_id = self.connection_count
        print(f"[Proxy] Connection #{connection_id} accepted from {client_socket.getpeername()}")

        try:
            # Connect to target server
            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_socket.connect((self.target_host, self.target_port))
            print(f"[Proxy] Connection #{connection_id} established to {self.target_host}:{self.target_port}")

            # Reset byte counter for new connection
            self.bytes_transferred = 0

            # Start forwarding threads
            client_to_server = threading.Thread(
                target=self.forward_data,
                args=(client_socket, server_socket, f"Client->Server #{connection_id}")
            )
            server_to_client = threading.Thread(
                target=self.forward_data,
                args=(server_socket, client_socket, f"Server->Client #{connection_id}")
            )

            client_to_server.daemon = True
            server_to_client.daemon = True

            client_to_server.start()
            server_to_client.start()

            # Wait for threads to finish
            client_to_server.join()
            server_to_client.join()

        except Exception as e:
            print(f"[Proxy] Error in connection #{connection_id}: {e}")
        finally:
            try:
                client_socket.close()
                server_socket.close()
            except:
                pass
            print(f"[Proxy] Connection #{connection_id} closed")

    def forward_data(self, source, destination, direction):
        try:
            while self.running:
                data = source.recv(4096)
                if not data:
                    break

                destination.send(data)
                self.bytes_transferred += len(data)

                print(f"[Proxy] {direction}: {len(data)} bytes, total: {self.bytes_transferred}")

                # Check if we should disconnect
                if self.bytes_transferred >= self.disconnect_after_bytes:
                    print(f"[Proxy] {direction}: Reached {self.disconnect_after_bytes} bytes, forcing disconnect")
                    break

        except Exception as e:
            print(f"[Proxy] Error forwarding {direction}: {e}")
        finally:
            try:
                source.close()
                destination.close()
            except:
                pass

    def start(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind(('0.0.0.0', self.listen_port))
        server_socket.listen(5)

        print(f"[Proxy] Listening on port {self.listen_port}, forwarding to {self.target_host}:{self.target_port}")
        print(f"[Proxy] Will disconnect after {self.disconnect_after_bytes} bytes")

        try:
            while self.running:
                client_socket, addr = server_socket.accept()
                client_thread = threading.Thread(target=self.handle_client, args=(client_socket,))
                client_thread.daemon = True
                client_thread.start()
        except KeyboardInterrupt:
            print("[Proxy] Shutting down...")
        finally:
            self.running = False
            server_socket.close()

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: python3 binlog_proxy.py <listen_port> <target_host> <target_port> <disconnect_after_mb>")
        sys.exit(1)

    listen_port = int(sys.argv[1])
    target_host = sys.argv[2]
    target_port = int(sys.argv[3])
    disconnect_after_mb = int(sys.argv[4])
    disconnect_after_bytes = disconnect_after_mb * 1024 * 1024

    proxy = BinlogProxy(listen_port, target_host, target_port, disconnect_after_bytes)
    proxy.start()
EOF

chmod +x /tmp/binlog_proxy.py

# Запуск прокси (разрыв каждые 5MB для более частых переподключений)
python3 /tmp/binlog_proxy.py $PROXY_PORT localhost $BINLOG_SERVER_PORT 5 > $PROXY_LOG 2>&1 &
proxy_pid=$!
echo "Started proxy with PID: $proxy_pid"
sleep 2

# Настройка MySQL slave для подключения через прокси
mysql -e "STOP SLAVE"
mysql -e "SET GLOBAL SERVER_ID = 123"
mysql -e "CHANGE MASTER TO MASTER_HOST=\"127.0.0.1\", MASTER_PORT=$PROXY_PORT, MASTER_USER=\"walg\", MASTER_PASSWORD=\"walgpwd\", MASTER_AUTO_POSITION=1"
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

# Ожидание завершения репликации с мониторингом переподключений
echo "Waiting for replication to complete with automatic reconnections..."
MAX_WAIT=120
WAIT_COUNT=0
EXPECTED_ROWS=2001  # 1 начальная + 2000 сгенерированных
RECONNECTION_COUNT=0

while [ $WAIT_COUNT -lt $MAX_WAIT ]; do
    ROW_COUNT=$(mysql -N -e "SELECT COUNT(*) FROM sbtest.pitr")
    SLAVE_IO_RUNNING=$(mysql -e "SHOW SLAVE STATUS\G" | awk '/Slave_IO_Running:/ {print $2}')
    SLAVE_SQL_RUNNING=$(mysql -e "SHOW SLAVE STATUS\G" | awk '/Slave_SQL_Running:/ {print $2}')

    echo "Row count: $ROW_COUNT / $EXPECTED_ROWS, IO: $SLAVE_IO_RUNNING, SQL: $SLAVE_SQL_RUNNING"

    if [ "$ROW_COUNT" -eq "$EXPECTED_ROWS" ]; then
        echo "Replication completed successfully"
        break
    fi

    # Подсчет переподключений из логов прокси
    CURRENT_RECONNECTIONS=$(grep -c "Connection #" $PROXY_LOG 2>/dev/null || echo "0")
    if [ "$CURRENT_RECONNECTIONS" -gt "$RECONNECTION_COUNT" ]; then
        echo "Detected reconnection #$CURRENT_RECONNECTIONS"
        RECONNECTION_COUNT=$CURRENT_RECONNECTIONS
    fi

    if [ "$SLAVE_SQL_RUNNING" != "Yes" ]; then
        echo "ERROR: Slave SQL thread stopped unexpectedly"
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

# Остановка прокси и сервера
echo "Stopping proxy (PID: $proxy_pid)..."
if kill -0 $proxy_pid 2>/dev/null; then
    kill -TERM $proxy_pid 2>/dev/null || true
fi

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

# Анализ результатов
echo "=== Test Results ==="
echo "Proxy log analysis:"
PROXY_CONNECTIONS=$(grep -c "Connection #" $PROXY_LOG 2>/dev/null || echo "0")
PROXY_DISCONNECTS=$(grep -c "forcing disconnect" $PROXY_LOG 2>/dev/null || echo "0")

echo "Proxy connections: $PROXY_CONNECTIONS"
echo "Proxy forced disconnects: $PROXY_DISCONNECTS"

echo "Binlog server log analysis:"
BINLOG_CONNECTIONS=$(grep -c 'connection accepted from' "$BINLOG_SERVER_LOG" || true)
BINLOG_RECONNECTS=$(grep -c 'Returning existing streamer for reconnection' "$BINLOG_SERVER_LOG" || true)

echo "Binlog server connections: $BINLOG_CONNECTIONS"
echo "Binlog server reconnections: $BINLOG_RECONNECTS"

# Проверка успешности теста
if [ "$PROXY_CONNECTIONS" -gt 1 ] && [ "$BINLOG_RECONNECTS" -gt 0 ]; then
    echo "SUCCESS: Multiple reconnections detected!"
    echo "- Proxy handled $PROXY_CONNECTIONS connections"
    echo "- Binlog server handled $BINLOG_RECONNECTS reconnections"
elif [ "$PROXY_CONNECTIONS" -gt 1 ]; then
    echo "SUCCESS: Multiple connections through proxy detected"
else
    echo "WARNING: Expected multiple connections, got proxy connections: $PROXY_CONNECTIONS"
    echo "Proxy log:"
    cat $PROXY_LOG
    echo "Binlog server log:"
    cat $BINLOG_SERVER_LOG
fi

echo "Test passed!"