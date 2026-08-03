#!/bin/sh
set -e -x

. /tmp/tests/test_functions/prepare_config.sh
prepare_config "/tmp/configs/journals_test_config.json"

get_journal_count() {
    wal-g --config=${TMP_CONFIG} st ls basebackups_005/ 2>&1 | grep journal_ | awk '{ printf $7 "\n" }' | wc -l
}

get_journal_name() {
    wal-g --config=${TMP_CONFIG} st ls basebackups_005/ 2>&1 | grep journal_ | awk '{ printf $7 "\n"}' | sort | awk "FNR == $1 {print}"
}

get_backup_name() {
    JOURNAL_NAME=$(get_journal_name $1)
    echo ${JOURNAL_NAME#"journal_"}
}

get_journal_size() {
    JOURNAL_NAME=$(get_journal_name $1)
    wal-g --config=${TMP_CONFIG} st cat basebackups_005/$JOURNAL_NAME | jq '.SizeToNextBackup'
}

force_new_wal() {
    if awk 'BEGIN {exit !('"$PG_VERSION"' >= 10)}'; then
        echo 'select pg_switch_wal();' | psql postgres
    else
        echo 'select pg_switch_xlog();' | psql postgres
    fi
    sleep 1
}

initdb ${PGDATA}
PG_VERSION=$(cat "${PGDATA}/PG_VERSION")

echo "archive_mode = on" >> ${PGDATA}/postgresql.conf
echo "archive_command = '/usr/bin/timeout 600 wal-g --config=${TMP_CONFIG} wal-push %p'" >> ${PGDATA}/postgresql.conf
echo "archive_timeout = 600" >> ${PGDATA}/postgresql.conf

pg_ctl -D ${PGDATA} -w start

wal-g --config=${TMP_CONFIG} st rm / --target=all || true

# Create backup #1 with journals
pgbench -i -s 1 postgres
force_new_wal
wal-g --config=${TMP_CONFIG} backup-push ${PGDATA} --count-journals
sleep 1

# Check count of backups and content of them
test "1" -eq $(get_journal_count)
test "0" -eq $(get_journal_size 1)

# Create backup #2 with journals
pgbench -i -s 1 postgres
force_new_wal
wal-g --config=${TMP_CONFIG} backup-push ${PGDATA} --count-journals
sleep 1

# Check count of backups and content of them
test "2" -eq $(get_journal_count)
test "0" -ne $(get_journal_size 1)
test "0" -eq $(get_journal_size 2)

# Create backup #3 with journals
pgbench -i -s 1 postgres
force_new_wal
wal-g --config=${TMP_CONFIG} backup-push ${PGDATA} --count-journals
sleep 1

# Check count of backups and content of them
test "3" -eq $(get_journal_count)

FIRST_BACKUP_SIZE=$(get_journal_size 1)
test "0" -ne $FIRST_BACKUP_SIZE

SECOND_BACKUP_SIZE=$(get_journal_size 2)
test "0" -ne $SECOND_BACKUP_SIZE

THIRD_BACKUP_SIZE=$(get_journal_size 3)
test "0" -eq $THIRD_BACKUP_SIZE

# We can successfully delete backup in the middle
wal-g --config=${TMP_CONFIG} delete target $(get_backup_name 2) --confirm
test "2" -eq $(get_journal_count)

NEW_FIRST_BACKUP_SIZE=$(get_journal_size 1)
test $NEW_FIRST_BACKUP_SIZE -eq $(($SECOND_BACKUP_SIZE + $FIRST_BACKUP_SIZE))

NEW_SECOND_BACKUP_SIZE=$(get_journal_size 2)
test "0" -eq $NEW_SECOND_BACKUP_SIZE

# We can successfully delete the last backup
wal-g --config=${TMP_CONFIG} delete target $(get_backup_name 2) --confirm
test "1" -eq $(get_journal_count)
test "0" -eq $(get_journal_size 1)

# We can successfully delete the single backup
wal-g --config=${TMP_CONFIG} delete target $(get_backup_name 1) --confirm
test "0" -eq $(get_journal_count)

/tmp/scripts/drop_pg.sh
rm ${TMP_CONFIG}
