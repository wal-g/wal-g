#!/bin/sh
set -e -x

. /usr/local/export_common.sh

export WALE_S3_PREFIX=s3://mysqlfullxtrabackupbucket

check_metadata() {
    if [ "$2" != "$3" ]; then
        printf 'Backup metadata mismatch for %s: expected <%s>, got <%s>\n' "$1" "$2" "$3" >&2
        exit 1
    fi
}

mysql_initialize_and_start

sysbench --table-size=10 prepare

sysbench --time=5 run

# add another uuid so have multline gtid_set in metadata for mysql5.7 
mysql -e "SET SESSION GTID_NEXT='aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1';
          BEGIN;
          COMMIT;
          SET SESSION GTID_NEXT='AUTOMATIC';"

mysql -e 'FLUSH LOGS'

mysqldump sbtest > /tmp/dump_before_backup

wal-g backup-push

backup_name=$(wal-g backup-list | awk 'NR==2{print $1}')
if [ -z "$backup_name" ]; then
    echo "No backup name found in wal-g backup-list after backup-push" >&2
    exit 1
fi

mysql_kill_and_clean_data

wal-g backup-fetch "$backup_name"

chown -R mysql:mysql $MYSQLDATA

mysql_start

mysql_set_gtid_purged

mysqldump sbtest > /tmp/dump_after_restore

diff /tmp/dump_before_backup /tmp/dump_after_restore


## check wal-g backup metadata

# retreive wal-g backup metadata
wal-g st cat "basebackups_005/${backup_name}_backup_stop_sentinel.json" > /tmp/backup_sentinel.json
sentinel_binlog=$(jq -r '.BinLogFileName // empty' /tmp/backup_sentinel.json)
sentinel_position=$(jq -r '.BinLogFilePosition // empty' /tmp/backup_sentinel.json)
sentinel_gtids=$(jq -r '.BinLogLastGTID // empty' /tmp/backup_sentinel.json)

# retreive xtrabackup backup metadata 
backup_binlog_info=$(tr -d '\n' < "$MYSQLDATA/xtrabackup_binlog_info")
backup_binlog=$(printf '%s' "$backup_binlog_info" | awk '{print $1}')
backup_position=$(printf '%s' "$backup_binlog_info" | awk '{print $2}')
backup_gtids=$(printf '%s' "$backup_binlog_info" | awk '{print $3}')

# normalize gtid_set before comparison 
backup_gtids=$(mysql --batch --skip-column-names -e "SELECT GTID_SUBTRACT('$backup_gtids', '')")
sentinel_gtids=$(mysql --batch --skip-column-names -e "SELECT GTID_SUBTRACT('$sentinel_gtids', '')")

check_metadata BinLogFileName "$backup_binlog" "$sentinel_binlog"
check_metadata BinLogFilePosition "$backup_position" "$sentinel_position"
check_metadata BinLogLastGTID "$backup_gtids" "$sentinel_gtids"
