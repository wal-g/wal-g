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

mysql -e "CREATE TABLE sbtest.pitr(id VARCHAR(32), ts DATETIME)"
mysql -e "FLUSH BINARY LOGS"

EMPTY_BINLOG=$(mysql_current_binlog)
mysql -e "FLUSH BINARY LOGS"
wal-g binlog-push
sleep 1

# WAL-G must skip the backup rows and apply the post-backup transactions.
# XtraBackup may rotate binlogs, so capture filenames at each boundary.
BACKUP_ROWS_BINLOG=$(mysql_current_binlog)
GTIDS_BEFORE_BACKUP_ROWS=$(mysql -N -e "SELECT @@GLOBAL.GTID_EXECUTED")
mysql -e "INSERT INTO sbtest.pitr VALUES('backup_and_binlog_01', NOW())"
GTIDS_AFTER_BACKUP_ROW_01=$(mysql -N -e "SELECT @@GLOBAL.GTID_EXECUTED")
BACKUP_AND_BINLOG_01_GTID=$(mysql -N -e "SELECT GTID_SUBTRACT('$GTIDS_AFTER_BACKUP_ROW_01', '$GTIDS_BEFORE_BACKUP_ROWS')")
mysql -e "INSERT INTO sbtest.pitr VALUES('backup_and_binlog_02', NOW())"
GTIDS_AFTER_BACKUP_ROW_02=$(mysql -N -e "SELECT @@GLOBAL.GTID_EXECUTED")
BACKUP_AND_BINLOG_02_GTID=$(mysql -N -e "SELECT GTID_SUBTRACT('$GTIDS_AFTER_BACKUP_ROW_02', '$GTIDS_AFTER_BACKUP_ROW_01')")
test -n "$BACKUP_AND_BINLOG_01_GTID"
test -n "$BACKUP_AND_BINLOG_02_GTID"
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

BINLOG_SERVER_LOG=/tmp/binlog_server_gtid_skip.log

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

mysqldump sbtest > /tmp/dump_after_pitr_gtid_skip

# rows from backup 
grep -w 'backup_and_binlog_01' /tmp/dump_after_pitr_gtid_skip
grep -w 'backup_and_binlog_02' /tmp/dump_after_pitr_gtid_skip
# rows from post-backup binlogs before pitr time 
grep -w 'from_binlog_01' /tmp/dump_after_pitr_gtid_skip
grep -w 'from_binlog_02' /tmp/dump_after_pitr_gtid_skip
grep -w 'from_binlog_03' /tmp/dump_after_pitr_gtid_skip
grep -w 'from_binlog_04' /tmp/dump_after_pitr_gtid_skip
grep -w 'from_binlog_05' /tmp/dump_after_pitr_gtid_skip
# rows after pitr time must be absent
if grep -w 'after_pitr_01' /tmp/dump_after_pitr_gtid_skip ||
    grep -w 'after_pitr_02' /tmp/dump_after_pitr_gtid_skip ||
    grep -w 'after_pitr_03' /tmp/dump_after_pitr_gtid_skip; then
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

grep -E "Skipping already-applied transaction ${BACKUP_AND_BINLOG_01_GTID}$" "$BINLOG_SERVER_LOG"
grep -E "Skipping already-applied transaction ${BACKUP_AND_BINLOG_02_GTID}$" "$BINLOG_SERVER_LOG"
