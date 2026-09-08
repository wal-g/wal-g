#!/bin/sh
set -eu

# shellcheck disable=SC1091
. /usr/local/export_common.sh

# Separate runs make an entirely tagged stream catch the empty-sentGTIDs bug,
# while a mixed stream checks that ordinary and tagged GTIDs coexist.
if [ "$#" -eq 0 ]; then
    for mode in plain tagged mixed; do
        "$0" "$mode"
    done
    exit 0
fi
mode=$1
case "$mode" in
    plain) backup_gtid_next=AUTOMATIC; first_gtid_next=AUTOMATIC; second_gtid_next=AUTOMATIC ;;
    tagged) backup_gtid_next=AUTOMATIC:review; first_gtid_next=AUTOMATIC:review; second_gtid_next=AUTOMATIC:review ;;
    mixed) backup_gtid_next=AUTOMATIC:review; first_gtid_next=AUTOMATIC; second_gtid_next=AUTOMATIC:review ;;
    *) echo "Unknown GTID test mode: $mode" >&2; exit 1 ;;
esac
echo "Testing binlog-server PITR with $mode GTIDs"

export WALE_S3_PREFIX="s3://mysql-pitr-binlogserver-bucket/$mode"
export WALG_COMPRESSION_METHOD=lz4
export WALG_MYSQL_BINLOG_SERVER_HOST="127.0.0.1"
export WALG_MYSQL_BINLOG_SERVER_PORT=9306
export WALG_MYSQL_BINLOG_SERVER_USER="walg"
export WALG_MYSQL_BINLOG_SERVER_PASSWORD="walgpwd"
export WALG_MYSQL_BINLOG_SERVER_ID=99
export WALG_MYSQL_BINLOG_SERVER_REPLICA_SOURCE="sbtest:@/sbtest"

walg_pid=
binlog_server_log="/tmp/mysql-binlog-server-$mode.log"

cleanup() {
    cleanup_status=$?
    trap - EXIT INT TERM

    if [ "$cleanup_status" -ne 0 ]; then
        mysql_show_replica_status >&2 || true
    fi

    mysql_stop_replica >/dev/null 2>&1 || true
    if [ -n "$walg_pid" ]; then
        kill "$walg_pid" >/dev/null 2>&1 || true
        wait "$walg_pid" >/dev/null 2>&1 || true
    fi
    mysql_stop || true

    if [ "$cleanup_status" -ne 0 ]; then
        test ! -f /var/log/mysql/error.log || cat /var/log/mysql/error.log >&2
        test ! -f "$binlog_server_log" || cat "$binlog_server_log" >&2
    fi

    exit "$cleanup_status"
}
trap cleanup EXIT
trap 'exit 1' INT TERM

wait_for_binlog_server_log() {
    wait_count=0
    while [ "$wait_count" -lt 120 ]; do
        if ! kill -0 "$walg_pid" 2>/dev/null; then
            echo "binlog-server exited while waiting for: $1" >&2
            exit 1
        fi
        if grep -Fq "$1" "$binlog_server_log"; then
            return 0
        fi
        wait_count=$((wait_count + 1))
        sleep 1
    done
    echo "Timed out waiting for binlog-server: $1" >&2
    exit 1
}

mysql_kill_and_clean_data
mysql_initialize_and_start

mysql_cache_server_version
case "$WALG_MYSQL_TEST_SERVER_VERSION" in
    8.4.*|9.7.*) ;;
    *)
        echo "Expected MySQL 8.4 or 9.7, got $WALG_MYSQL_TEST_SERVER_VERSION" >&2
        exit 1
        ;;
esac

# Upload closed binlogs before the backup so the sentinel has a valid starting
# point, then create changes that must be restored by binlog-server.
mysql -e "CREATE TABLE sbtest.pitr(id VARCHAR(32) PRIMARY KEY, ts DATETIME)"
mysql -e "FLUSH BINARY LOGS"
mysql -e "FLUSH BINARY LOGS"
wal-g binlog-push

before_backup_gtids=$(mysql --batch --skip-column-names -e "SELECT @@GLOBAL.gtid_executed")
mysql -e "SET SESSION gtid_next='$backup_gtid_next'; INSERT INTO sbtest.pitr VALUES('from_backup', NOW())"
backup_gtid=$(mysql --batch --skip-column-names -e "SELECT GTID_SUBTRACT(@@GLOBAL.gtid_executed, '$before_backup_gtids')")
wal-g backup-push

mysql -e "SET SESSION gtid_next='$first_gtid_next'; INSERT INTO sbtest.pitr VALUES('from_binlog_01', NOW())"
mysql -e "SET SESSION gtid_next='$second_gtid_next'; INSERT INTO sbtest.pitr VALUES('from_binlog_02', NOW())"
expected_gtids=$(mysql --batch --skip-column-names -e "SELECT @@GLOBAL.gtid_executed")
sleep 1
pitr_time=$(date3339)
sleep 1
mysql -e "SET SESSION gtid_next='$second_gtid_next'; INSERT INTO sbtest.pitr VALUES('after_pitr', NOW())"
mysql -e "FLUSH BINARY LOGS"
wal-g binlog-push

# Restore the physical XtraBackup and initialize the restored GTID state before
# replication starts from the WAL-G binlog-server.
mysql_kill_and_clean_data
wal-g backup-fetch LATEST
chown -R mysql:mysql "$MYSQLDATA"
mysql_start
mysql_set_gtid_purged

WALG_LOG_LEVEL=DEVEL wal-g binlog-server --since LATEST --until "$pitr_time" \
    > "$binlog_server_log" 2>&1 &
walg_pid=$!

# This message is emitted only after net.Listen succeeds.
wait_for_binlog_server_log "Listening on "
mysql -e "SET GLOBAL SERVER_ID = 123"
mysql_change_replication_source "127.0.0.1" 9306 "walg" "walgpwd"

# Hold the SQL thread back until streaming has finished. The server must keep
# running while tagged transactions have been received but not yet applied.
mysql -e "START REPLICA IO_THREAD"
wait_for_binlog_server_log "Waiting for replica to catch up to GTID:"
kill -0 "$walg_pid"
grep -F "Skipping already-applied transaction $backup_gtid" "$binlog_server_log"
mysql -e "START REPLICA SQL_THREAD"

wait_count=0
while kill -0 "$walg_pid" 2>/dev/null; do
    if [ "$wait_count" -ge 120 ]; then
        echo "Timed out waiting for binlog-server replication" >&2
        exit 1
    fi
    wait_count=$((wait_count + 1))
    sleep 1
done

# A crash is an error; a successful but premature exit must fail the exact
# snapshot check below, without waiting for the replica to apply more rows.
wait "$walg_pid"
walg_pid=
restored_rows=$(mysql --batch --skip-column-names -e "SELECT id, COUNT(*) FROM sbtest.pitr GROUP BY id ORDER BY id")
expected_rows=$(printf '%s\t1\n' from_backup from_binlog_01 from_binlog_02)
if [ "$restored_rows" != "$expected_rows" ]; then
    printf 'Unexpected rows after PITR (%s):\n%s\n' "$mode" "$restored_rows" >&2
    exit 1
fi
restored_gtids=$(mysql --batch --skip-column-names -e "SELECT GTID_SUBSET('$expected_gtids', @@GLOBAL.gtid_executed)")
test "$restored_gtids" = 1
