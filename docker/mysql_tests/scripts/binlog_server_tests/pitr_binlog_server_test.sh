#!/bin/sh
set -e -x

# shellcheck disable=SC1091
. /usr/local/export_common.sh

export WALE_S3_PREFIX=s3://mysql-pitr-binlogserver-bucket
export WALG_MYSQL_BINLOG_SERVER_HOST="localhost"
export WALG_MYSQL_BINLOG_SERVER_PORT=9306
export WALG_MYSQL_BINLOG_SERVER_USER="walg"
export WALG_MYSQL_BINLOG_SERVER_PASSWORD="walgpwd"
export WALG_MYSQL_BINLOG_SERVER_ID=99
export WALG_MYSQL_BINLOG_SERVER_REPLICA_SOURCE="sbtest@tcp(127.0.0.1:3306)/sbtest"

mysql_initialize_and_start

mysql -e "CREATE TABLE sbtest.pitr(id VARCHAR(32) PRIMARY KEY, ts DATETIME)"
mysql -e "FLUSH BINARY LOGS"

EMPTY_BINLOG=$(mysql_current_binlog)
mysql -e "FLUSH BINARY LOGS"
wal-g binlog-push
sleep 1

# The replica must skip the backup rows by GTID and apply post-backup transactions.
# XtraBackup may rotate binlogs, so capture filenames at each boundary.
BACKUP_ROWS_BINLOG=$(mysql_current_binlog)
mysql -e "INSERT INTO sbtest.pitr VALUES('backup_and_binlog_01', NOW())"
mysql -e "INSERT INTO sbtest.pitr VALUES('backup_and_binlog_02', NOW())"
wal-g backup-push
FIRST_REPLAY_BINLOG=$(mysql_current_binlog)
mysql -e "INSERT INTO sbtest.pitr VALUES('from_binlog_01', NOW())"
mysql -e "INSERT INTO sbtest.pitr VALUES('from_binlog_02', NOW())"
mysql -e "FLUSH BINARY LOGS"

SECOND_REPLAY_BINLOG=$(mysql_current_binlog)
mysql -e "INSERT INTO sbtest.pitr VALUES('from_binlog_03', NOW())"
mysql -e "INSERT INTO sbtest.pitr VALUES('from_binlog_04', NOW())"
mysql -e "FLUSH BINARY LOGS"

PITR_CUTOFF_BINLOG=$(mysql_current_binlog)
mysql -e "INSERT INTO sbtest.pitr VALUES('from_binlog_05', NOW())"
sleep 1
DT1=$(date3339)
sleep 1
mysql -e "INSERT INTO sbtest.pitr VALUES('after_pitr_01', NOW())"
mysql -e "FLUSH BINARY LOGS"

AFTER_PITR_BINLOG=$(mysql_current_binlog)
mysql -e "INSERT INTO sbtest.pitr VALUES('after_pitr_02', NOW())"
mysql -e "INSERT INTO sbtest.pitr VALUES('after_pitr_03', NOW())"
mysql -e "FLUSH BINARY LOGS"
wal-g binlog-push

mysql_kill_and_clean_data
wal-g backup-fetch LATEST
chown -R mysql:mysql "$MYSQLDATA"
mysql_start
mysql_set_gtid_purged

BINLOG_SERVER_LOG=/tmp/pitr_binlog_server.log

WALG_LOG_LEVEL="DEVEL" wal-g binlog-server --since LATEST --until "$DT1" > "$BINLOG_SERVER_LOG" 2>&1 &
walg_pid=$!
trap 'kill "$walg_pid" 2>/dev/null || true' EXIT
trap 'exit 1' INT TERM

sleep 3
mysql_stop_replica
mysql -e "SET GLOBAL SERVER_ID = 123"
mysql_change_replication_source "127.0.0.1" 9306 "walg" "walgpwd"
mysql_start_replica

if wait "$walg_pid"; then
    trap - EXIT INT TERM
else
    walg_status=$?
    trap - EXIT INT TERM
    cat "$BINLOG_SERVER_LOG" >&2
    exit "$walg_status"
fi
cat "$BINLOG_SERVER_LOG"

mysqldump sbtest > /tmp/dump_after_pitr

# rows from backup
grep -w 'backup_and_binlog_01' /tmp/dump_after_pitr
grep -w 'backup_and_binlog_02' /tmp/dump_after_pitr
# rows from post-backup binlogs before pitr time
grep -w 'from_binlog_01' /tmp/dump_after_pitr
grep -w 'from_binlog_02' /tmp/dump_after_pitr
grep -w 'from_binlog_03' /tmp/dump_after_pitr
grep -w 'from_binlog_04' /tmp/dump_after_pitr
grep -w 'from_binlog_05' /tmp/dump_after_pitr
# rows after pitr time must be absent
if grep -w 'after_pitr_01' /tmp/dump_after_pitr ||
    grep -w 'after_pitr_02' /tmp/dump_after_pitr ||
    grep -w 'after_pitr_03' /tmp/dump_after_pitr; then
    echo "ERROR: found rows written after the PITR cutoff"
    exit 1
fi

for binlog in "$EMPTY_BINLOG" "$BACKUP_ROWS_BINLOG" "$FIRST_REPLAY_BINLOG" "$SECOND_REPLAY_BINLOG" "$PITR_CUTOFF_BINLOG"; do
    grep -F "Streaming $WALG_MYSQL_BINLOG_DST/$binlog to replica" "$BINLOG_SERVER_LOG"
done

if grep -F "Streaming $WALG_MYSQL_BINLOG_DST/$AFTER_PITR_BINLOG to replica" "$BINLOG_SERVER_LOG"; then
    echo "ERROR: streamed $AFTER_PITR_BINLOG after reaching the PITR cutoff"
    exit 1
fi

# Replayed backup transactions must not create duplicates.
test "$(mysql -N -e 'SELECT COUNT(*) FROM sbtest.pitr')" = 7
