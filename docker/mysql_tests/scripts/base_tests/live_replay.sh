#!/bin/sh
set -e -x

. /usr/local/export_common.sh

export WALE_S3_PREFIX=s3://mysqllivereplay
export WALG_MYSQL_BINLOG_REPLAY_COMMAND='echo "Ok" >> "$WALG_MYSQL_CURRENT_BINLOG.ok" ; while [ -d "$WALG_MYSQL_BINLOG_DST" ] && [ ! -f "$WALG_MYSQL_CURRENT_BINLOG.in" ]; do sleep 1; done'
replay_dir=$(mktemp -d)
export WALG_MYSQL_BINLOG_DST="$replay_dir"
replay_pid=

cleanup() {
    cleanup_status=$?
    trap - EXIT INT TERM
    if [ -n "$replay_pid" ]; then
        kill "$replay_pid" 2>/dev/null || true
        wait "$replay_pid" 2>/dev/null || true
    fi
    # Removing the directory also releases a replay command left waiting after
    # WAL-G was terminated, so it cannot outlive this test indefinitely.
    rm -rf "$replay_dir"
    exit "$cleanup_status"
}
trap cleanup EXIT
trap 'exit 1' INT TERM

wait_for_replay() {
    replay_wait_count=0
    while [ ! -f "$replay_dir/$1.ok" ]; do
        if ! kill -0 "$replay_pid" 2>/dev/null; then
            echo "binlog-replay exited before replaying $1" >&2
            exit 1
        fi
        if [ "$replay_wait_count" -ge 60 ]; then
            echo "Timed out waiting for binlog-replay to reach $1" >&2
            exit 1
        fi
        replay_wait_count=$((replay_wait_count + 1))
        sleep 1
    done
}

mysql_initialize_and_start
wal-g backup-push
sleep 1

mysql -e "CREATE TABLE sbtest.pitr(id VARCHAR(32), ts DATETIME)"
#  REPLAY_COMMAND may lag behind DOWNLOAD for 'binlogFetchAhead' (internal/databases/mysql/binlog_replay_handler.go)
#  so, we are making a lot of binlog files:
for idx in 1 2 3 4 5 6 7
do
mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr$idx', NOW())"
mysql -e "FLUSH LOGS"
wal-g binlog-push
done
sleep 1

wal-g binlog-replay --until "2030-01-01T00:00:00.000000000+00:00" &
replay_pid=$!

binlogs=$(mysql --batch --skip-column-names -e "SHOW BINARY LOGS")
first_binlog=$(printf '%s\n' "$binlogs" | awk 'NR == 1 {print $1}')
test -n "$first_binlog"
wait_for_replay "$first_binlog"

# Keep replay blocked while another closed binlog is uploaded.
mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr_last', NOW())"
mysql -e "FLUSH LOGS"
wal-g binlog-push
sleep 1

# XtraBackup versions rotate binlogs a different number of times. Release all
# closed binlogs reported by MySQL; the last entry is still being written.
binlogs=$(mysql --batch --skip-column-names -e "SHOW BINARY LOGS")
closed_binlogs=$(printf '%s\n' "$binlogs" | awk 'NR > 1 {print previous} {previous = $1}')
test -n "$closed_binlogs"
for binlog in $closed_binlogs; do
    wait_for_replay "$binlog"
    echo "proceed" > "$replay_dir/$binlog.in"
done

wait "$replay_pid"
replay_pid=
