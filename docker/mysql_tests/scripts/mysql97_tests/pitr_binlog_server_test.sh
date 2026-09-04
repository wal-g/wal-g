#!/bin/sh
set -eu

# shellcheck disable=SC1091
. /usr/local/export_common.sh

export WALE_S3_PREFIX=s3://mysql-pitr-binlogserver-bucket
export WALG_COMPRESSION_METHOD=lz4
export WALG_MYSQL_DATASOURCE_NAME="sbtest:@/sbtest"
export WALG_STREAM_CREATE_COMMAND="mysqldump --single-transaction --set-gtid-purged=ON --add-drop-database --databases sbtest"
export WALG_STREAM_RESTORE_COMMAND="mysql --no-defaults --user=root"
export WALG_MYSQL_BACKUP_PREPARE_COMMAND=
export WALG_MYSQL_BINLOG_SERVER_HOST="127.0.0.1"
export WALG_MYSQL_BINLOG_SERVER_PORT=9306
export WALG_MYSQL_BINLOG_SERVER_USER="walg"
export WALG_MYSQL_BINLOG_SERVER_PASSWORD="walgpwd"
export WALG_MYSQL_BINLOG_SERVER_ID=99
export WALG_MYSQL_BINLOG_SERVER_REPLICA_SOURCE="sbtest:@/sbtest"

walg_pid=
mysqld_pid=

cleanup() {
    cleanup_status=$?
    trap - EXIT INT TERM

    mysql_stop_replica >/dev/null 2>&1 || true
    if [ -n "$walg_pid" ]; then
        kill "$walg_pid" >/dev/null 2>&1 || true
        wait "$walg_pid" >/dev/null 2>&1 || true
    fi
    if [ -n "$mysqld_pid" ]; then
        kill "$mysqld_pid" >/dev/null 2>&1 || true
        wait "$mysqld_pid" >/dev/null 2>&1 || true
    fi

    if [ "$cleanup_status" -ne 0 ]; then
        mysql_show_replica_status >&2 || true
        test ! -f /var/lib/mysql/error.log || cat /var/lib/mysql/error.log >&2
        test ! -f /tmp/mysql97-binlog-server.log || cat /tmp/mysql97-binlog-server.log >&2
    fi

    exit "$cleanup_status"
}
trap cleanup EXIT
trap 'exit 1' INT TERM

rm -rf "${MYSQLDATA:?}"/*
mysqld --initialize-insecure --user=mysql
mysqld --user=mysql > /var/lib/mysql/error.log 2>&1 &
mysqld_pid=$!

wait_count=0
until mysqladmin --no-defaults --user=root ping >/dev/null 2>&1; do
    wait_count=$((wait_count + 1))
    if [ "$wait_count" -ge 60 ]; then
        echo "MySQL did not become ready" >&2
        exit 1
    fi
    sleep 1
done

mysql --no-defaults --user=root < /etc/mysql/init.sql

mysql_cache_server_version
case "$WALG_MYSQL_TEST_SERVER_VERSION" in
    9.7.*) ;;
    *)
        echo "Expected MySQL 9.7, got $WALG_MYSQL_TEST_SERVER_VERSION" >&2
        exit 1
        ;;
esac

# Upload closed binlogs before the backup so the sentinel has a valid starting
# point, then create changes that must be restored by binlog-server.
mysql -e "CREATE TABLE sbtest.pitr(id VARCHAR(32), ts DATETIME)"
mysql -e "FLUSH BINARY LOGS"
mysql -e "FLUSH BINARY LOGS"
wal-g binlog-push

mysql -e "INSERT INTO sbtest.pitr VALUES('from_backup', NOW())"
wal-g backup-push

mysql -e "INSERT INTO sbtest.pitr VALUES('from_binlog_01', NOW())"
mysql -e "INSERT INTO sbtest.pitr VALUES('from_binlog_02', NOW())"
sleep 1
pitr_time=$(date3339)
sleep 1
mysql -e "INSERT INTO sbtest.pitr VALUES('after_pitr', NOW())"
mysql -e "FLUSH BINARY LOGS"
wal-g binlog-push

# Logical restore stays online. Resetting the executed GTID set lets the dump
# restore its GTID_PURGED value before replication starts from the backup.
mysql -e "DROP DATABASE sbtest"
mysql_reset_binary_logs_and_gtids
wal-g backup-fetch LATEST

WALG_LOG_LEVEL=DEVEL wal-g binlog-server --since LATEST --until "$pitr_time" \
    > /tmp/mysql97-binlog-server.log 2>&1 &
walg_pid=$!

sleep 3
mysql -e "SET GLOBAL SERVER_ID = 123"
mysql_change_replication_source "127.0.0.1" 9306 "walg" "walgpwd"
mysql_start_replica

wait_count=0
while [ "$wait_count" -lt 120 ]; do
    restored_rows=$(mysql --batch --skip-column-names -e "SELECT COUNT(*) FROM sbtest.pitr" 2>/dev/null || printf '0')
    if [ "$restored_rows" -ge 3 ]; then
        break
    fi
    wait_count=$((wait_count + 1))
    sleep 1
done

if [ "$restored_rows" -lt 3 ]; then
    echo "Timed out waiting for binlog-server replication" >&2
    exit 1
fi

wait "$walg_pid"
walg_pid=

mysqldump --skip-comments sbtest > /tmp/mysql97-dump-after-pitr.sql
grep -w 'from_backup' /tmp/mysql97-dump-after-pitr.sql
grep -w 'from_binlog_01' /tmp/mysql97-dump-after-pitr.sql
grep -w 'from_binlog_02' /tmp/mysql97-dump-after-pitr.sql
if grep -w 'after_pitr' /tmp/mysql97-dump-after-pitr.sql; then
    echo "Found a row written after the PITR cutoff" >&2
    exit 1
fi
