#!/bin/sh
set -e -x

. /usr/local/export_common.sh

export WALE_S3_PREFIX=s3://mysql_journal_test_bucket

get_journal_count() {
    wal-g st ls basebackups_005/ 2>&1 | grep journal_ | awk '{ printf $7 "\n" }' | wc -l
}

get_journal_name() {
    wal-g st ls basebackups_005/ 2>&1 | grep journal_ | awk '{ printf $7 "\n"}' | sort | awk "FNR == $1 {print}"
}

get_backup_name() {
    JOURNAL_NAME=$(get_journal_name $1)
    echo ${JOURNAL_NAME#"journal_"}
}

get_journal_size() {
    JOURNAL_NAME=$(get_journal_name $1)
    wal-g st cat basebackups_005/$JOURNAL_NAME | jq '.SizeToNextBackup'
}

mysqld --initialize --init-file=/etc/mysql/init.sql
service mysql start
sysbench --table-size=10 prepare

# Create backup #1 with journals
sysbench --time=1 run
mysql -e 'FLUSH LOGS'
wal-g binlog-push
wal-g backup-push --count-journals
sleep 1

# Check count of backups and content of them
test "1" -eq $(get_journal_count)
test "0" -eq $(get_journal_size 1)

# Create backup #2 with journals
sysbench --time=1 run
mysql -e 'FLUSH LOGS'
wal-g binlog-push
wal-g backup-push --count-journals
sleep 1

# Check count of backups and content of them
test "2" -eq $(get_journal_count)
test "0" -ne $(get_journal_size 1)
test "0" -eq $(get_journal_size 2)

# Create backup #3 with journals
sysbench --time=1 run
mysql -e 'FLUSH LOGS'
wal-g binlog-push
wal-g backup-push --count-journals
sleep 1

# Check count of backups and content of them
test "3" -eq $(get_journal_count)

FIRST_BACKUP_SIZE=$(get_journal_size 1)
test "0" -ne $FIRST_BACKUP_SIZE

SECOND_BACKUP_SIZE=$(get_journal_size 2)
test "0" -ne $SECOND_BACKUP_SIZE

THIRD_BACKUP_SIZE=$(get_journal_size 3)
test "0" -eq $THIRD_BACKUP_SIZE

# We can sucessfully delete backup in the middle
wal-g delete target $(get_backup_name 2) --confirm
test "2" -eq $(get_journal_count)

NEW_FIRST_BACKUP_SIZE=$(get_journal_size 1)
test "0" -ne $FIRST_BACKUP_SIZE

NEW_THIRD_BACKUP_SIZE=$(get_journal_size 2)
test "0" -eq $NEW_THIRD_BACKUP_SIZE

test $NEW_FIRST_BACKUP_SIZE -eq $(($SECOND_BACKUP_SIZE + $FIRST_BACKUP_SIZE))

# We can sucessfully delete the last backup
wal-g delete target $(get_backup_name 2) --confirm
test "1" -eq $(get_journal_count)
test "0" -eq $(get_journal_size 1)

# We can sucessfully delete the single backup
wal-g delete target $(get_backup_name 1) --confirm
test "0" -eq $(get_journal_count)
