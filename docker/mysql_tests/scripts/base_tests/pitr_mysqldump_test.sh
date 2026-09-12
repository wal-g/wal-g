#!/bin/sh
set -e -x

. /usr/local/export_common.sh

export WALE_S3_PREFIX=s3://mysqlpitrmysqldumpbucket
# Restore the application database and its snapshot GTIDs, not the freshly
# initialized server's system tables. GTIDs are needed to skip transactions
# already present in the dump when replay starts at a binlog file boundary.
export WALG_STREAM_CREATE_COMMAND="mysqldump --databases sbtest --single-transaction --set-gtid-purged=ON"
export WALG_STREAM_RESTORE_COMMAND="mysql"
export WALG_MYSQL_BACKUP_PREPARE_COMMAND=

mysql_initialize_and_start

# Give both backups a known starting binlog in storage.
mysql -e "FLUSH BINARY LOGS; FLUSH BINARY LOGS"
wal-g binlog-push

# first full backup
wal-g backup-push
FIRST_BACKUP=$(wal-g backup-list | awk 'NR==2{print $1}')
sleep 1

mysql -e "CREATE TABLE sbtest.pitr(id VARCHAR(32) PRIMARY KEY, ts DATETIME)"
mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr01', NOW())"
mysql -e "FLUSH LOGS"
wal-g binlog-push
mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr02', NOW())"
sleep 1

# second full backup
wal-g backup-push
mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr03', NOW())"
EXPECTED_GTIDS=$(mysql --batch --skip-column-names -e "SELECT @@GLOBAL.gtid_executed")
sleep 1

DT1=$(date3339)

sleep 1
mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr04', NOW())"
mysql -e "FLUSH LOGS"
wal-g binlog-push


# Check both PITR after the latest backup and replay across another full backup.
for backup in LATEST "$FIRST_BACKUP"; do
    mysql_kill_and_clean_data
    mysql_initialize_and_start
    # Initialization generated fresh GTIDs. Clear them before the dump installs
    # its own GTID_PURGED set; the dump disables sql_log_bin while restoring.
    mysql_reset_binary_logs_and_gtids
    wal-g backup-fetch "$backup"

    wal-g binlog-replay --since "$backup" --until "$DT1"
    actual_rows=$(mysql --batch --skip-column-names -e "SELECT id, COUNT(*) FROM sbtest.pitr GROUP BY id ORDER BY id")
    expected_rows=$(printf '%s\t1\n' testpitr01 testpitr02 testpitr03)
    if [ "$actual_rows" != "$expected_rows" ]; then
        printf 'Unexpected rows after PITR from %s:\n%s\n' "$backup" "$actual_rows" >&2
        exit 1
    fi
    restored_gtids=$(mysql --batch --skip-column-names -e "SELECT GTID_SUBSET('$EXPECTED_GTIDS', @@GLOBAL.gtid_executed)")
    test "$restored_gtids" = 1
done
