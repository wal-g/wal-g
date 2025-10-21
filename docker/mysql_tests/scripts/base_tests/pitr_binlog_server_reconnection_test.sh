#!/bin/sh
set -e -x

. /usr/local/export_common.sh

export WALE_S3_PREFIX=s3://mysql_pitr_binlogserver_reconnection_bucket
export WALG_MYSQL_BINLOG_SERVER_HOST="localhost"
export WALG_MYSQL_BINLOG_SERVER_PORT=9307
export WALG_MYSQL_BINLOG_SERVER_USER="walg"
export WALG_MYSQL_BINLOG_SERVER_PASSWORD="walgpwd"
export WALG_MYSQL_BINLOG_SERVER_ID=99
export WALG_MYSQL_BINLOG_SERVER_REPLICA_SOURCE="sbtest@tcp(127.0.0.1:3306)/sbtest"

mysqld --initialize --init-file=/etc/mysql/init.sql
service mysql start

wal-g backup-push

mysql -e "CREATE TABLE sbtest.reconnection_test(id VARCHAR(32), ts DATETIME, batch_num INT)"
mysql -e "INSERT INTO sbtest.reconnection_test VALUES('batch1_01', NOW(), 1)"
mysql -e "INSERT INTO sbtest.reconnection_test VALUES('batch1_02', NOW(), 1)"
mysql -e "FLUSH LOGS"
wal-g binlog-push

mysql -e "INSERT INTO sbtest.reconnection_test VALUES('batch2_01', NOW(), 2)"
mysql -e "INSERT INTO sbtest.reconnection_test VALUES('batch2_02', NOW(), 2)"
sleep 1
DT1=$(date3339)
sleep 1
mysql -e "INSERT INTO sbtest.reconnection_test VALUES('batch3_01', NOW(), 3)"
mysql -e "INSERT INTO sbtest.reconnection_test VALUES('batch3_02', NOW(), 3)"
mysql -e "FLUSH LOGS"
wal-g binlog-push

mysql_kill_and_clean_data
wal-g backup-fetch LATEST
chown -R mysql:mysql $MYSQLDATA
service mysql start || (cat /var/log/mysql/error.log && false)
mysql_set_gtid_purged

echo "=== Starting binlog-server ==="
WALG_LOG_LEVEL="DEVEL" wal-g binlog-server --since LATEST --until "$DT1" &
walg_pid=$!

sleep 3

if ! kill -0 $walg_pid 2>/dev/null; then
    echo "ERROR: binlog-server failed to start"
    exit 1
fi

echo "=== First connection ==="
mysql -e "STOP SLAVE"
mysql -e "SET GLOBAL SERVER_ID = 123"
mysql -e "CHANGE MASTER TO MASTER_HOST=\"127.0.0.1\", MASTER_PORT=9307, MASTER_USER=\"walg\", MASTER_PASSWORD=\"walgpwd\", MASTER_AUTO_POSITION=1"
mysql -e "START SLAVE"

sleep 5

echo "=== Checking replication status after first connection ==="
mysql -e "SHOW SLAVE STATUS\G" | grep -E "(Slave_IO_Running|Slave_SQL_Running|Last_Error)" || true

REPLICATED_COUNT=$(mysql -sN -e "SELECT COUNT(*) FROM sbtest.reconnection_test WHERE batch_num <= 2" 2>/dev/null || echo "0")
echo "Replicated records after first connection: $REPLICATED_COUNT"

echo "=== Testing reconnection scenario 1: STOP/START SLAVE ==="
mysql -e "STOP SLAVE"
sleep 3

if ! kill -0 $walg_pid 2>/dev/null; then
    echo "ERROR: binlog-server died after STOP SLAVE"
    exit 1
fi

echo "binlog-server still running after STOP SLAVE"

mysql -e "START SLAVE"
sleep 3

echo "=== Checking replication status after reconnection ==="
mysql -e "SHOW SLAVE STATUS\G" | grep -E "(Slave_IO_Running|Slave_SQL_Running|Last_Error)" || true

echo "=== Testing reconnection scenario 2: Connection reset ==="
mysql -e "STOP SLAVE"
mysql -e "RESET SLAVE"
sleep 2

if ! kill -0 $walg_pid 2>/dev/null; then
    echo "ERROR: binlog-server died after RESET SLAVE"
    exit 1
fi

echo "binlog-server still running after RESET SLAVE"

mysql -e "CHANGE MASTER TO MASTER_HOST=\"127.0.0.1\", MASTER_PORT=9307, MASTER_USER=\"walg\", MASTER_PASSWORD=\"walgpwd\", MASTER_AUTO_POSITION=1"
mysql -e "START SLAVE"
sleep 3

echo "=== Testing reconnection scenario 3: Multiple quick reconnections ==="
for i in 1 2 3; do
    echo "Quick reconnection attempt $i"
    mysql -e "STOP SLAVE"
    sleep 1
    mysql -e "START SLAVE"
    sleep 2

    if ! kill -0 $walg_pid 2>/dev/null; then
        echo "ERROR: binlog-server died during quick reconnection $i"
        exit 1
    fi
done

echo "binlog-server survived multiple quick reconnections"

echo "=== Waiting for replication to complete ==="
wait $walg_pid

echo "=== Verifying final results ==="
mysqldump sbtest > /tmp/dump_after_reconnection_test

grep -w 'batch1_01' /tmp/dump_after_reconnection_test
grep -w 'batch1_02' /tmp/dump_after_reconnection_test
grep -w 'batch2_01' /tmp/dump_after_reconnection_test
grep -w 'batch2_02' /tmp/dump_after_reconnection_test

! grep -w 'batch3_01' /tmp/dump_after_reconnection_test
! grep -w 'batch3_02' /tmp/dump_after_reconnection_test

FINAL_COUNT=$(mysql -sN -e "SELECT COUNT(*) FROM sbtest.reconnection_test")
echo "Final record count: $FINAL_COUNT"

if [ "$FINAL_COUNT" -eq 4 ]; then
    echo "=== RECONNECTION TEST PASSED ==="
else
    echo "ERROR: Expected 4 records, got $FINAL_COUNT"
    exit 1
fi