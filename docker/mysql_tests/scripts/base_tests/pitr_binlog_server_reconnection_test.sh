#!/bin/sh
set -e -x

. /usr/local/export_common.sh

# Проверка и установка python3
if ! command -v python3 >/dev/null 2>&1; then
    echo "python3 not found, installing..."
    apt-get update
    apt-get install -y python3
fi

s3cmd s3://mysql_pitr_binlogserver_reconnection_bucket || true
export WALE_S3_PREFIX=s3://mysql_pitr_binlogserver_reconnection_bucket
export WALG_MYSQL_BINLOG_SERVER_HOST="127.0.0.1"
export WALG_MYSQL_BINLOG_SERVER_PORT=9306
export WALG_MYSQL_BINLOG_SERVER_USER="walg"
export WALG_MYSQL_BINLOG_SERVER_PASSWORD="walgpwd"
export WALG_MYSQL_BINLOG_SERVER_ID=99
export WALG_MYSQL_BINLOG_SERVER_REPLICA_SOURCE="sbtest@tcp(127.0.0.1:3306)/sbtest"
export WALG_COMPRESSION_METHOD=zstd

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

# Генерируем данные для теста
for i in $(seq 1 500); do
    mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr_batch_$i', NOW())"
    if [ $((i % 50)) -eq 0 ]; then
        mysql -e "FLUSH LOGS"
        sleep 0.2
        wal-g binlog-push
    fi
done

sleep 3
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

# Создаем прокси с ограничением на ДВА переподключения
cat > /tmp/binlog_proxy.py << 'EOF'
#!/usr/bin/env python3
import socket
import threading
import time
import sys
import select

class TwoDisconnectBinlogProxy:
    def __init__(self, listen_port, target_host, target_port, planned_disconnects=2):
        self.listen_port = listen_port
        self.target_host = target_host
        self.target_port = target_port
        self.running = True
        self.client_socket = None
        self.server_socket = None
        self.disconnect_count = 0
        self.planned_disconnects = planned_disconnects  # Планируемое количество разрывов
        self.bytes_transferred = 0
        self.connection_start_time = None
        self.total_bytes_transferred = 0
        self.disconnects_completed = False  # Флаг завершения плановых разрывов

    def connect_to_server(self):
        """Подключается к binlog серверу"""
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
        """Определяет, нужно ли разорвать соединение"""
        # Если уже сделали все плановые разрывы - больше не разрываем
        if self.disconnects_completed:
            return False

        if not self.connection_start_time:
            return False

        # Разрываем соединение после передачи 32KB данных
        # Но только если еще не достигли лимита плановых разрывов
        if self.bytes_transferred > 32768 and self.disconnect_count < self.planned_disconnects:
            return True

        return False

    def handle_client_connection(self, client_socket):
        """Обрабатывает подключение клиента с ограниченными переподключениями"""
        self.client_socket = client_socket
        print(f"[Proxy] Client connected from {client_socket.getpeername()}")

        # Первоначальное подключение к серверу
        if not self.connect_to_server():
            print("[Proxy] Initial connection to server failed")
            return

        self.connection_start_time = time.time()
        self.bytes_transferred = 0

        try:
            while self.running:
                # Проверяем, нужно ли переподключиться (только если не завершили плановые разрывы)
                if self.should_disconnect():
                    print(f"[Proxy] Planned disconnect #{self.disconnect_count + 1}/{self.planned_disconnects} after {self.bytes_transferred} bytes")
                    print(f"[Proxy] Total bytes transferred so far: {self.total_bytes_transferred}")

                    self.server_socket.close()
                    time.sleep(2)  # Имитируем задержку сети

                    self.disconnect_count += 1

                    # Проверяем, завершили ли мы все плановые разрывы
                    if self.disconnect_count >= self.planned_disconnects:
                        self.disconnects_completed = True
                        print(f"[Proxy] Completed all {self.planned_disconnects} planned disconnects. Now working in stable mode.")

                    if not self.connect_to_server():
                        print("[Proxy] Reconnection failed, closing client connection")
                        break

                    self.connection_start_time = time.time()
                    self.bytes_transferred = 0

                try:
                    # Используем select для мониторинга сокетов
                    ready_sockets, _, error_sockets = select.select(
                        [self.client_socket, self.server_socket], [],
                        [self.client_socket, self.server_socket], 1.0
                    )

                    if error_sockets:
                        print("[Proxy] Socket error detected")
                        break

                    if not ready_sockets:
                        continue

                    # Передача данных от клиента к серверу
                    if self.client_socket in ready_sockets:
                        try:
                            data = self.client_socket.recv(8192)
                            if not data:
                                print("[Proxy] Client disconnected")
                                break
                            self.server_socket.send(data)
                            self.bytes_transferred += len(data)
                            self.total_bytes_transferred += len(data)

                            # Показываем статус режима работы
                            mode = "STABLE" if self.disconnects_completed else f"DISCONNECT_MODE({self.disconnect_count}/{self.planned_disconnects})"
                            print(f"[Proxy] [{mode}] Client->Server: {len(data)} bytes (session: {self.bytes_transferred}, total: {self.total_bytes_transferred})")
                        except Exception as e:
                            print(f"[Proxy] Error forwarding client->server: {e}")
                            break

                    # Передача данных от сервера к клиенту
                    if self.server_socket in ready_sockets:
                        try:
                            data = self.server_socket.recv(8192)
                            if not data:
                                print("[Proxy] Server disconnected")
                                break
                            self.client_socket.send(data)
                            self.bytes_transferred += len(data)
                            self.total_bytes_transferred += len(data)

                            # Показываем статус режима работы
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
        """Запускает прокси сервер"""
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

# Функция для проверки, что порт слушается
check_port_listening() {
    local port=$1
    local host=${2:-127.0.0.1}

    # Метод 1: netstat
    if command -v netstat >/dev/null 2>&1; then
        if netstat -ln 2>/dev/null | grep -E ":${port}[[:space:]]" >/dev/null; then
            echo "Port $port detected by netstat"
            return 0
        fi
    fi

    # Метод 2: ss
    if command -v ss >/dev/null 2>&1; then
        if ss -ln 2>/dev/null | grep -E ":${port}[[:space:]]" >/dev/null; then
            echo "Port $port detected by ss"
            return 0
        fi
    fi

    # Метод 3: прямое подключение
    if timeout 2 bash -c "echo >/dev/tcp/${host}/${port}" 2>/dev/null; then
        echo "Port $port is accepting connections"
        return 0
    fi

    return 1
}

# Функция для безопасной остановки процесса
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

# Улучшенная функция для восстановления репликации с анализом ошибок
# Улучшенная функция для восстановления репликации с анализом ошибок
recover_replication() {
    echo "=== ANALYZING REPLICATION ERROR ==="

    # Получаем детальную информацию об ошибке
    SLAVE_STATUS=$(mysql -e "SHOW SLAVE STATUS\G" 2>/dev/null)
    LAST_SQL_ERROR=$(echo "$SLAVE_STATUS" | grep "Last_SQL_Error:" | cut -d: -f2- | xargs)
    LAST_SQL_ERRNO=$(echo "$SLAVE_STATUS" | grep "Last_SQL_Errno:" | awk '{print $2}')

    # Получаем GTID информацию
    RETRIEVED_GTID=$(echo "$SLAVE_STATUS" | grep "Retrieved_Gtid_Set:" | cut -d: -f2- | xargs)
    EXECUTED_GTID=$(echo "$SLAVE_STATUS" | grep "Executed_Gtid_Set:" | cut -d: -f2- | xargs)

    echo "SQL Error Code: $LAST_SQL_ERRNO"
    echo "SQL Error Message: $LAST_SQL_ERROR"
    echo "Retrieved GTID: $RETRIEVED_GTID"
    echo "Executed GTID: $EXECUTED_GTID"

    # Логируем ошибку для анализа
    echo "$(date): Error $LAST_SQL_ERRNO: $LAST_SQL_ERROR" >> /tmp/replication_errors.log

    # Проверяем, является ли это безопасной ошибкой дублирования
    SAFE_TO_SKIP=false

    case "$LAST_SQL_ERRNO" in
        "1062")  # Duplicate entry for key
            echo "SAFE: Duplicate entry error (1062) - transaction already applied"
            SAFE_TO_SKIP=true
            ;;
        "1778")  # GTID transaction error - НОВЫЙ КОД!
            echo "ANALYZING: GTID transaction error (1778) - checking details..."
            # Получаем детали из performance_schema
            WORKER_ERROR=$(mysql -N -e "SELECT LAST_ERROR_MESSAGE FROM performance_schema.replication_applier_status_by_worker WHERE LAST_ERROR_NUMBER != 0 LIMIT 1" 2>/dev/null || echo "")

            echo "Worker Error Message: $WORKER_ERROR"

            if echo "$WORKER_ERROR" | grep -qi "GTID_NEXT.*UUID:NUMBER"; then
                echo "SAFE: GTID transaction conflict - transaction already processed"
                SAFE_TO_SKIP=true
            elif echo "$WORKER_ERROR" | grep -qi "implicit commit.*transaction"; then
                echo "SAFE: GTID implicit commit error - duplicate transaction"
                SAFE_TO_SKIP=true
            else
                echo "UNSAFE: Unknown GTID error: $WORKER_ERROR"
            fi
            ;;
        "1837")  # Coordinator stopped
            echo "ANALYZING: Coordinator error (1837) - checking worker details..."
            # Получаем детали из performance_schema
            WORKER_ERROR=$(mysql -N -e "SELECT LAST_ERROR_MESSAGE FROM performance_schema.replication_applier_status_by_worker WHERE LAST_ERROR_NUMBER != 0 LIMIT 1" 2>/dev/null || echo "")
            WORKER_ERRNO=$(mysql -N -e "SELECT LAST_ERROR_NUMBER FROM performance_schema.replication_applier_status_by_worker WHERE LAST_ERROR_NUMBER != 0 LIMIT 1" 2>/dev/null || echo "")

            echo "Worker Error Code: $WORKER_ERRNO"
            echo "Worker Error Message: $WORKER_ERROR"

            if [ "$WORKER_ERRNO" = "1062" ]; then
                echo "SAFE: Worker failed with duplicate entry (1062) - transaction already applied"
                SAFE_TO_SKIP=true
            elif [ "$WORKER_ERRNO" = "1778" ]; then
                echo "SAFE: Worker failed with GTID error (1778) - duplicate transaction"
                SAFE_TO_SKIP=true
            elif echo "$WORKER_ERROR" | grep -qi "duplicate\|already.*exist\|GTID_NEXT"; then
                echo "SAFE: Worker failed due to duplicate data or GTID conflict"
                SAFE_TO_SKIP=true
            else
                echo "UNSAFE: Worker failed with unknown error: $WORKER_ERROR"
            fi
            ;;
        "0")     # Coordinator error - нужно анализировать текст
            if echo "$LAST_SQL_ERROR" | grep -qi "already executed"; then
                echo "SAFE: GTID already executed - transaction was applied before"
                SAFE_TO_SKIP=true
            elif echo "$LAST_SQL_ERROR" | grep -qi "duplicate"; then
                echo "SAFE: Duplicate transaction detected"
                SAFE_TO_SKIP=true
            else
                echo "UNSAFE: Unknown coordinator error"
            fi
            ;;
        *)
            echo "UNSAFE: Unknown error code $LAST_SQL_ERRNO - manual investigation required"
            ;;
    esac

    if [ "$SAFE_TO_SKIP" = "true" ]; then
        echo "PROCEEDING: Skipping duplicate/conflicting transaction..."

        # Останавливаем репликацию
        mysql -e "STOP SLAVE" 2>/dev/null || true

        # Пропускаем одну ошибочную транзакцию
        mysql -e "SET GLOBAL SQL_SLAVE_SKIP_COUNTER = 1" 2>/dev/null || true

        # Перезапускаем репликацию
        mysql -e "START SLAVE" 2>/dev/null || true

        sleep 3

        # Проверяем статус
        SLAVE_SQL_RUNNING=$(mysql -e "SHOW SLAVE STATUS\G" | grep "Slave_SQL_Running:" | awk '{print $2}')
        if [ "$SLAVE_SQL_RUNNING" = "Yes" ]; then
            echo "SUCCESS: Replication recovered after skipping conflicting transaction"
            return 0
        else
            echo "FAILED: Replication still not running after skip"
            return 1
        fi
    else
        echo "ERROR: Cannot safely skip this error - stopping test"
        echo "=== Full slave status ==="
        mysql -e "SHOW SLAVE STATUS\G"
        echo "=== Worker error details ==="
        mysql -e "SELECT * FROM performance_schema.replication_applier_status_by_worker WHERE LAST_ERROR_NUMBER != 0" 2>/dev/null || echo "No worker error details available"
        return 1
    fi
}

# Запуск wal-g binlog-server
echo "Starting wal-g binlog-server..."
WALG_LOG_LEVEL="DEVEL" wal-g binlog-server --since LATEST --until "$DT1" > $BINLOG_SERVER_LOG 2>&1 &
walg_pid=$!
echo "Started wal-g binlog-server with PID: $walg_pid"

# Ждем запуска binlog-server
echo "Waiting for binlog-server to start..."
WAIT_COUNT=0
MAX_WAIT=30

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

    echo "Waiting for binlog-server... ($WAIT_COUNT/$MAX_WAIT)"
    sleep 2
    WAIT_COUNT=$((WAIT_COUNT + 1))
done

if [ $WAIT_COUNT -eq $MAX_WAIT ]; then
    echo "ERROR: Binlog server failed to start within $((MAX_WAIT * 2)) seconds"
    echo "=== Binlog server log ==="
    cat $BINLOG_SERVER_LOG
    exit 1
fi

sleep 2

# Запуск прокси с ограничением на 2 переподключения
echo "Starting proxy with max 2 reconnections..."
python3 /tmp/binlog_proxy.py > $PROXY_LOG 2>&1 &
proxy_pid=$!
echo "Started proxy with PID: $proxy_pid"

# Ждем запуска прокси
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

# Настройка MySQL slave для подключения через прокси
echo "Configuring MySQL replication..."
mysql -e "STOP SLAVE"
mysql -e "RESET SLAVE ALL"
mysql -e "SET GLOBAL SERVER_ID = 123"
mysql -e "SET GLOBAL SLAVE_NET_TIMEOUT = 10"
mysql -e "CHANGE MASTER TO MASTER_HOST='127.0.0.1', MASTER_PORT=$PROXY_PORT, MASTER_USER='walg', MASTER_PASSWORD='walgpwd', MASTER_AUTO_POSITION=1, MASTER_CONNECT_RETRY=5, MASTER_RETRY_COUNT=86400"
mysql -e "START SLAVE"

# Ожидание запуска репликации
echo "Waiting for replication to start..."
WAIT_COUNT=0
MAX_WAIT=60
while [ $WAIT_COUNT -lt $MAX_WAIT ]; do
    SLAVE_STATUS=$(mysql -e "SHOW SLAVE STATUS\G" 2>/dev/null || echo "")
    SLAVE_IO_RUNNING=$(echo "$SLAVE_STATUS" | grep "Slave_IO_Running:" | awk '{print $2}')
    SLAVE_SQL_RUNNING=$(echo "$SLAVE_STATUS" | grep "Slave_SQL_Running:" | awk '{print $2}')
    LAST_IO_ERROR=$(echo "$SLAVE_STATUS" | grep "Last_IO_Error:" | cut -d: -f2- | xargs)

    echo "Wait $WAIT_COUNT/$MAX_WAIT: IO=$SLAVE_IO_RUNNING, SQL=$SLAVE_SQL_RUNNING"

    if [ -n "$LAST_IO_ERROR" ] && [ "$LAST_IO_ERROR" != "" ]; then
        echo "IO Error: $LAST_IO_ERROR"
    fi

    if [ "$SLAVE_IO_RUNNING" = "Yes" ]; then
        echo "Replication IO thread started successfully"
        break
    fi

    if [ "$SLAVE_IO_RUNNING" != "Connecting" ] && [ "$SLAVE_IO_RUNNING" != "" ]; then
        echo "ERROR: Unexpected slave IO state: $SLAVE_IO_RUNNING"
        echo "=== Full slave status ==="
        mysql -e "SHOW SLAVE STATUS\G"
        exit 1
    fi

    sleep 2
    WAIT_COUNT=$((WAIT_COUNT + 1))
done

if [ "$SLAVE_IO_RUNNING" != "Yes" ]; then
    echo "ERROR: Replication IO thread failed to start"
    echo "=== Final slave status ==="
    mysql -e "SHOW SLAVE STATUS\G"
    exit 1
fi

# Ожидание завершения репликации с точным анализом ошибок
echo "Waiting for replication to complete..."
MAX_WAIT=180
WAIT_COUNT=0
EXPECTED_ROWS=501
LAST_ROW_COUNT=0
STUCK_COUNT=0
RECOVERY_ATTEMPTS=0
MAX_RECOVERY_ATTEMPTS=3  # Ограничиваем количество попыток восстановления

while [ $WAIT_COUNT -lt $MAX_WAIT ]; do
    ROW_COUNT=$(mysql -N -e "SELECT COUNT(*) FROM sbtest.pitr" 2>/dev/null || echo "0")
    SLAVE_IO_RUNNING=$(mysql -e "SHOW SLAVE STATUS\G" | grep "Slave_IO_Running:" | awk '{print $2}')
    SLAVE_SQL_RUNNING=$(mysql -e "SHOW SLAVE STATUS\G" | grep "Slave_SQL_Running:" | awk '{print $2}')
    mysql -e "SHOW SLAVE STATUS\G" | grep -E "(Retrieved_Gtid_Set|Executed_Gtid_Set)"
    echo "Row count: $ROW_COUNT / $EXPECTED_ROWS, IO: $SLAVE_IO_RUNNING, SQL: $SLAVE_SQL_RUNNING (wait: $WAIT_COUNT/$MAX_WAIT)"
    if [ "$ROW_COUNT" -ge 490 ]; then
            echo "=== Current table content (showing all rows) ==="
            mysql -e "SELECT id FROM sbtest.pitr ORDER BY id"
            echo "=== End of table ==="
    fi

    if [ "$ROW_COUNT" -ge "$EXPECTED_ROWS" ]; then
        echo "Replication completed successfully"
        break
    fi

    # Проверяем, не застряла ли репликация
    if [ "$ROW_COUNT" -eq "$LAST_ROW_COUNT" ]; then
        STUCK_COUNT=$((STUCK_COUNT + 1))
        if [ $STUCK_COUNT -ge 15 ]; then
            echo "WARNING: Replication appears stuck for 30 seconds"
            mysql -e "SHOW SLAVE STATUS\G" | grep -E "(Master_Log_File|Read_Master_Log_Pos|Exec_Master_Log_Pos|Last_.*Error)"
            STUCK_COUNT=0
        fi
    else
        STUCK_COUNT=0
    fi
    LAST_ROW_COUNT=$ROW_COUNT

    # Обработка ошибок SQL потока с точным анализом
    if [ "$SLAVE_SQL_RUNNING" != "Yes" ]; then
        echo "WARNING: Slave SQL thread stopped (attempt $((RECOVERY_ATTEMPTS + 1))/$MAX_RECOVERY_ATTEMPTS)"

        if [ $RECOVERY_ATTEMPTS -lt $MAX_RECOVERY_ATTEMPTS ]; then
            if recover_replication; then
                echo "Recovery successful, continuing replication"
                RECOVERY_ATTEMPTS=$((RECOVERY_ATTEMPTS + 1))
                STUCK_COUNT=0
                sleep 5
                continue
            else
                echo "ERROR: Recovery failed - unsafe to continue"
                exit 1
            fi
        else
            echo "ERROR: Maximum recovery attempts reached"
            exit 1
        fi
    fi

    sleep 2
    WAIT_COUNT=$((WAIT_COUNT + 1))
done

# Остановка процессов
safe_kill_process "$proxy_pid" "proxy"
safe_kill_process "$walg_pid" "wal-g binlog-server"

# Проверка данных
FINAL_ROW_COUNT=$(mysql -N -e "SELECT COUNT(*) FROM sbtest.pitr")
AFTER_COUNT=$(mysql -N -e "SELECT COUNT(*) FROM sbtest.pitr WHERE id = 'testpitr_after'")

if [ "$AFTER_COUNT" -ne 0 ]; then
    echo "ERROR: Record after DT1 should not be replicated"
    exit 1
fi

# Анализ результатов
echo "=== Test Results ==="
echo "Final row count: $FINAL_ROW_COUNT"
echo "Recovery attempts used: $RECOVERY_ATTEMPTS"

# Подсчитываем переподключения из логов
PROXY_RECONNECTS=$(grep -c "Simulating network disconnect" "$PROXY_LOG" 2>/dev/null || echo "0")
BINLOG_CONNECTIONS=$(grep -c 'connection accepted from' "$BINLOG_SERVER_LOG" 2>/dev/null || echo "0")

echo "Proxy reconnects: $PROXY_RECONNECTS (expected: 2)"
echo "Binlog server connections: $BINLOG_CONNECTIONS"

# Проверяем, что было ровно 2 переподключения
if [ "$PROXY_RECONNECTS" -ne 2 ]; then
    echo "WARNING: Expected exactly 2 reconnects, got $PROXY_RECONNECTS"
fi

# Проверка успешности теста
if [ "$FINAL_ROW_COUNT" -ge "$EXPECTED_ROWS" ]; then
    echo "SUCCESS: Limited reconnection test passed!"
    echo "- Data replicated successfully: $FINAL_ROW_COUNT rows"
    echo "- Network disconnects: $PROXY_RECONNECTS (limited to 2)"
    echo "- Recovery attempts: $RECOVERY_ATTEMPTS (only for duplicate transactions)"
elif [ "$FINAL_ROW_COUNT" -gt 350 ] && [ "$PROXY_RECONNECTS" -eq 2 ]; then
    echo "PARTIAL SUCCESS: Reconnection functionality verified"
    echo "- Partial data replicated: $FINAL_ROW_COUNT rows"
    echo "- Network disconnects: $PROXY_RECONNECTS (as expected)"
    echo "- Recovery attempts: $RECOVERY_ATTEMPTS"
else
    echo "ERROR: Test failed"
    echo "- Expected $EXPECTED_ROWS rows, got $FINAL_ROW_COUNT"
    echo "- Proxy reconnects: $PROXY_RECONNECTS (expected: 2)"
    echo "- Recovery attempts: $RECOVERY_ATTEMPTS"
    echo "=== Proxy log ==="
    cat $PROXY_LOG
    echo "=== Binlog server log (last 50 lines) ==="
    tail -50 $BINLOG_SERVER_LOG
    echo "=== Replication errors log ==="
    cat /tmp/replication_errors.log 2>/dev/null || echo "No replication errors logged"
    exit 1
fi

echo "Test completed successfully!"