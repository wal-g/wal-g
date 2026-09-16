#!/bin/sh
set -e -x

# shellcheck disable=SC1091
. /usr/local/export_common.sh

export WALE_S3_PREFIX=s3://mysql-pitr-binlogserver-until-lmt-bucket
export WALG_MYSQL_BINLOG_SERVER_HOST="localhost"
export WALG_MYSQL_BINLOG_SERVER_PORT=9306
export WALG_MYSQL_BINLOG_SERVER_USER="walg"
export WALG_MYSQL_BINLOG_SERVER_PASSWORD="walgpwd"
export WALG_MYSQL_BINLOG_SERVER_ID=99
export WALG_MYSQL_BINLOG_SERVER_REPLICA_SOURCE="sbtest@tcp(127.0.0.1:3306)/sbtest"

mysql_initialize_and_start

BACKUP_ROWS_BINLOG=$(mysql_current_binlog)
mysql -e "CREATE TABLE sbtest.pitr(id VARCHAR(32), ts DATETIME)"
mysql -e "INSERT INTO sbtest.pitr VALUES('from_binlog_01', NOW())"
mysql -e "INSERT INTO sbtest.pitr VALUES('from_binlog_02', NOW())"
mysql -e "FLUSH BINARY LOGS"
wal-g backup-push

REPLAY_BINLOG=$(mysql_current_binlog)
mysql -e "INSERT INTO sbtest.pitr VALUES('from_binlog_03', NOW())"
mysql -e "INSERT INTO sbtest.pitr VALUES('from_binlog_04', NOW())"
mysql -e "FLUSH BINARY LOGS"
wal-g binlog-push

AFTER_LMT_BINLOG=$(mysql_current_binlog)
mysql -e "INSERT INTO sbtest.pitr VALUES('lmt_ignored_01', NOW())"
sleep 1
DT1=$(date3339)
sleep 1
mysql -e "INSERT INTO sbtest.pitr VALUES('after_pitr_01', NOW())"
mysql -e "FLUSH LOGS"
wal-g binlog-push

mysql_kill_and_clean_data
wal-g backup-fetch LATEST
chown -R mysql:mysql "$MYSQLDATA"
mysql_start
mysql_set_gtid_purged

BINLOG_SERVER_LOG=/tmp/binlog_server_until_lmt.log

# DT1 is used as both PITR time (--until) and the binlog last-modified cutoff
# (--until-binlog-last-modified-time). BACKUP_ROWS_BINLOG and REPLAY_BINLOG
# were pushed to S3 before DT1, so they are eligible. AFTER_LMT_BINLOG was
# pushed after DT1, so it must be filtered out by endBinlogTS even though
# lmt_ignored_01 (inside it) is valid data before PITR time.
WALG_LOG_LEVEL="DEVEL" wal-g binlog-server \
    --since LATEST \
    --until "$DT1" \
    --until-binlog-last-modified-time "$DT1" \
    > "$BINLOG_SERVER_LOG" 2>&1 &
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

mysqldump sbtest > /tmp/dump_after_pitr_until_lmt

# Rows from the backup and binlogs pushed before the LMT cutoff.
grep -w 'from_binlog_01' /tmp/dump_after_pitr_until_lmt
grep -w 'from_binlog_02' /tmp/dump_after_pitr_until_lmt
grep -w 'from_binlog_03' /tmp/dump_after_pitr_until_lmt
grep -w 'from_binlog_04' /tmp/dump_after_pitr_until_lmt

# lmt_ignored_01 is in AFTER_LMT_BINLOG, which was pushed to S3 after DT1 (LMT),
# so it must be absent even though the data is before PITR time
if grep -w 'lmt_ignored_01' /tmp/dump_after_pitr_until_lmt; then
    echo "ERROR: found row from a binlog beyond the last-modified cutoff"
    exit 1
fi

# rows after pitr time must be absent
if grep -w 'after_pitr_01' /tmp/dump_after_pitr_until_lmt; then
    echo "ERROR: found row written after the PITR cutoff"
    exit 1
fi

# Binlog numbers depend on the rotations performed by XtraBackup.
grep -F "Streaming $WALG_MYSQL_BINLOG_DST/$BACKUP_ROWS_BINLOG to replica" "$BINLOG_SERVER_LOG"
grep -F "Streaming $WALG_MYSQL_BINLOG_DST/$REPLAY_BINLOG to replica" "$BINLOG_SERVER_LOG"

# A file pushed after the LMT cutoff must not be streamed.
if grep -F "Streaming $WALG_MYSQL_BINLOG_DST/$AFTER_LMT_BINLOG to replica" "$BINLOG_SERVER_LOG"; then
    echo "ERROR: streamed $AFTER_LMT_BINLOG beyond the last-modified cutoff"
    exit 1
fi
