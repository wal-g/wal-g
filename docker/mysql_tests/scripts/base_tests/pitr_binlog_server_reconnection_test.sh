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
mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr01', NOW())"
mysql -e "FLUSH LOGS"
wal-g binlog-push

for i in $(seq 1 100); do
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
    echo "Replication started successfully"

    for attempt in $(seq 1 3); do
        sleep 1

        if ! kill -0 $walg_pid 2>/dev/null; then
            echo "Binlog server finished, stopping reconnection test"
            break
        fi

        echo "Reconnection attempt $attempt"
        mysql -e "STOP SLAVE IO_THREAD"
        sleep 1
        mysql -e "START SLAVE IO_THREAD"
        sleep 1

        SLAVE_IO_RUNNING=$(mysql -e "SHOW SLAVE STATUS\G" | grep "Slave_IO_Running: Yes" | wc -l)
        if [ "$SLAVE_IO_RUNNING" -eq 1 ]; then
            echo "Reconnection $attempt successful"
        else
            echo "ERROR: Reconnection $attempt failed"
            mysql -e "SHOW SLAVE STATUS\G"
            exit 1
        fi
    done
fi

wait $walg_pid || true

ROW_COUNT=$(mysql -N -e "SELECT COUNT(*) FROM sbtest.pitr")
EXPECTED_COUNT=101  # 1 + 100

if [ "$ROW_COUNT" -ne "$EXPECTED_COUNT" ]; then
    echo "ERROR: Expected $EXPECTED_COUNT rows, got $ROW_COUNT"
    exit 1
fi

AFTER_COUNT=$(mysql -N -e "SELECT COUNT(*) FROM sbtest.pitr WHERE id = 'testpitr_after'")
if [ "$AFTER_COUNT" -ne 0 ]; then
    echo "ERROR: Record after DT1 should not be replicated"
    exit 1
fi

echo "Test passed!"