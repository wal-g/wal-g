#!/bin/sh
set -e -x

. /usr/local/export_common.sh

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
for i in $(seq 1 50); do
    mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr_batch1_$i', NOW())"
done
mysql -e "FLUSH LOGS"
wal-g binlog-push

for i in $(seq 1 50); do
    mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr_batch2_$i', NOW())"
done
sleep 1
DT1=$(date3339)
sleep 1

for i in $(seq 1 50); do
    mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr_batch3_$i', NOW())"
done
mysql -e "FLUSH LOGS"
wal-g binlog-push

mysql_kill_and_clean_data
wal-g backup-fetch LATEST
chown -R mysql:mysql $MYSQLDATA
service mysql start || (cat /var/log/mysql/error.log && false)
mysql_set_gtid_purged

WALG_LOG_LEVEL="DEVEL" wal-g binlog-server --since LATEST --until "$DT1" &
walg_pid=$!

sleep 3
mysql -e "STOP SLAVE"
mysql -e "SET GLOBAL SERVER_ID = 123"
mysql -e "CHANGE MASTER TO MASTER_HOST=\"127.0.0.1\", MASTER_PORT=9306, MASTER_USER=\"walg\", MASTER_PASSWORD=\"walgpwd\", MASTER_AUTO_POSITION=1"
mysql -e "START SLAVE"

sleep 2

SLAVE_IO_RUNNING=$(mysql -e "SHOW SLAVE STATUS\G" | grep "Slave_IO_Running: Yes" | wc -l)
if [ "$SLAVE_IO_RUNNING" -eq 1 ]; then
    echo "Replication started, testing reconnection..."

    mysql -e "STOP SLAVE IO_THREAD"
    sleep 2

    mysql -e "START SLAVE IO_THREAD"
    sleep 3

    SLAVE_IO_RUNNING=$(mysql -e "SHOW SLAVE STATUS\G" | grep "Slave_IO_Running: Yes" | wc -l)
    if [ "$SLAVE_IO_RUNNING" -ne 1 ]; then
        echo "ERROR: Reconnection failed"
        mysql -e "SHOW SLAVE STATUS\G"
        exit 1
    fi
    echo "Reconnection successful!"
fi

wait $walg_pid

ROW_COUNT=$(mysql -N -e "SELECT COUNT(*) FROM sbtest.pitr WHERE id LIKE 'testpitr_batch1_%' OR id LIKE 'testpitr_batch2_%'")
if [ "$ROW_COUNT" -ne 100 ]; then
    echo "ERROR: Expected 100 rows, got $ROW_COUNT"
    exit 1
fi

BATCH3_COUNT=$(mysql -N -e "SELECT COUNT(*) FROM sbtest.pitr WHERE id LIKE 'testpitr_batch3_%'")
if [ "$BATCH3_COUNT" -ne 0 ]; then
    echo "ERROR: batch3 should not be replicated"
    exit 1
fi

echo "Test passed!"