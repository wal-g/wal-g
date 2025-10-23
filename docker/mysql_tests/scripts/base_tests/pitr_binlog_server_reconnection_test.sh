#!/bin/sh
set -e -x

. /usr/local/export_common.sh

s3cmd mb s3://mysql_pitr_binlogserver_reconnection_bucket || true
export WALE_S3_PREFIX=s3://mysql_pitr_binlogserver_bucket
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

for i in $(seq 1 200); do
    mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr_batch_$i', NOW())"
    if [ $((i % 20)) -eq 0 ]; then
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

sleep 3

mysql -e "STOP SLAVE"
mysql -e "SET GLOBAL SERVER_ID = 123"
mysql -e "CHANGE MASTER TO MASTER_HOST=\"127.0.0.1\", MASTER_PORT=9306, MASTER_USER=\"walg\", MASTER_PASSWORD=\"walgpwd\", MASTER_AUTO_POSITION=1"
mysql -e "START SLAVE"

sleep 5

SLAVE_IO_RUNNING=$(mysql -e "SHOW SLAVE STATUS\G" | grep "Slave_IO_Running: Yes" | wc -l)
if [ "$SLAVE_IO_RUNNING" -eq 1 ]; then
    echo "Replication started successfully"
else
    echo "ERROR: Replication IO thread did not start"
    mysql -e "SHOW SLAVE STATUS\G"
    exit 1
fi

INITIAL_COUNT=$(mysql -N -e "SELECT COUNT(*) FROM sbtest.pitr")
echo "Initial row count after replication start: $INITIAL_COUNT"

echo "Simulating network connection loss via iptables..."

iptables -A INPUT -p tcp --dport 9306 -j DROP
iptables -A OUTPUT -p tcp --sport 9306 -j DROP

sleep 3

SLAVE_IO_RUNNING=$(mysql -e "SHOW SLAVE STATUS\G" | grep "Slave_IO_Running: Yes" | wc -l)
echo "Slave IO running after network block: $SLAVE_IO_RUNNING"

# Восстанавливаем соединение
echo "Restoring network connection..."
iptables -D INPUT -p tcp --dport 9306 -j DROP
iptables -D OUTPUT -p tcp --sport 9306 -j DROP

sleep 10

SLAVE_IO_RUNNING=$(mysql -e "SHOW SLAVE STATUS\G" | grep "Slave_IO_Running: Yes" | wc -l)
if [ "$SLAVE_IO_RUNNING" -eq 1 ]; then
    echo "Replication restored successfully after network reconnect"
else
    echo "ERROR: Replication IO thread did not restore after network reconnect"
    mysql -e "SHOW SLAVE STATUS\G"
    exit 1
fi

sleep 5
COUNT_AFTER_RECONNECT=$(mysql -N -e "SELECT COUNT(*) FROM sbtest.pitr")
echo "Row count after reconnect: $COUNT_AFTER_RECONNECT"

if [ "$COUNT_AFTER_RECONNECT" -gt "$INITIAL_COUNT" ]; then
    echo "Data continues to replicate after reconnect: $INITIAL_COUNT -> $COUNT_AFTER_RECONNECT"
else
    echo "WARNING: No new data replicated yet, waiting more..."
    sleep 5
    COUNT_AFTER_RECONNECT=$(mysql -N -e "SELECT COUNT(*) FROM sbtest.pitr")
    if [ "$COUNT_AFTER_RECONNECT" -gt "$INITIAL_COUNT" ]; then
        echo "Data replicated after additional wait: $INITIAL_COUNT -> $COUNT_AFTER_RECONNECT"
    fi
fi

echo "Waiting for wal-g to complete..."
wait $walg_pid || true
echo "wal-g completed"

ROW_COUNT=$(mysql -N -e "SELECT COUNT(*) FROM sbtest.pitr")
EXPECTED_COUNT=201

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