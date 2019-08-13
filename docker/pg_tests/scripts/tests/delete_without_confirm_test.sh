#!/bin/sh
set -e -x
CONFIG_FILE="/tmp/configs/delete_without_confirm_test_config.json"
COMMON_CONFIG="/tmp/configs/common_config.json"
TMP_CONFIG="/tmp/configs/tmp_config.json"
cat ${CONFIG_FILE} > ${TMP_CONFIG}
echo "," >> ${TMP_CONFIG}
cat ${COMMON_CONFIG} >> ${TMP_CONFIG}

tmp/scripts/wrap_config_file.sh ${TMP_CONFIG}

WAL_PUSH_LOGS="/tmp/logs/wal_push_logs/pg_delete_without_confirm_test_logs"
WAL_FETCH_LOGS="/tmp/logs/wal_fetch_logs/pg_delete_without_confirm_test_logs"
BACKUP_PUSH_LOGS="/tmp/logs/backup_push_logs/pg_delete_without_confirm_test_logs"
BACKUP_FETCH_LOGS="/tmp/logs/backup_fetch_logs/pg_delete_without_confirm_test_logs"

/usr/lib/postgresql/10/bin/initdb ${PGDATA}

echo "archive_mode = on" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_command = '/usr/bin/timeout 600 /usr/bin/time -v -a --output ${WAL_PUSH_LOGS} /usr/bin/wal-g --config=${TMP_CONFIG} wal-push %p'" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_timeout = 600" >> /var/lib/postgresql/10/main/postgresql.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start

for i in 1 2
do
    pgbench -i -s 1 postgres &
    sleep 1
    /usr/bin/time -v -a --output ${BACKUP_PUSH_LOGS} wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}
done

lines_before_delete=`wal-g --config=${TMP_CONFIG} backup-list | wc -l`
wal-g --config=${TMP_CONFIG} backup-list > /tmp/list_before_delete

wal-g --config=${TMP_CONFIG} delete retain FULL 1

lines_after_delete=`wal-g --config=${TMP_CONFIG} backup-list | wc -l`
wal-g --config=${TMP_CONFIG} backup-list > /tmp/list_after_delete

if [ $lines_before_delete -ne $lines_after_delete ];
then
    echo $lines_before_delete > /tmp/before_delete
    echo $lines_after_delete > /tmp/after_delete
    echo "Backups were deleted without --confirm"
    diff /tmp/before_delete /tmp/after_delete
fi

diff /tmp/list_before_delete /tmp/list_after_delete

tmp/scripts/drop_pg.sh
rm ${TMP_CONFIG}
echo "Delete retain FULL success!!!!!!"
