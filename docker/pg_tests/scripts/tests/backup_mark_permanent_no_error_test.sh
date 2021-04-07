#!/bin/sh
set -e -x

CONFIG_FILE="/tmp/configs/backup_mark_permanent_no_error_test_config.json"

COMMON_CONFIG="/tmp/configs/common_config.json"
TMP_CONFIG="/tmp/configs/tmp_config.json"
cat ${CONFIG_FILE} > ${TMP_CONFIG}
echo "," >> ${TMP_CONFIG}
cat ${COMMON_CONFIG} >> ${TMP_CONFIG}
/tmp/scripts/wrap_config_file.sh ${TMP_CONFIG}

/usr/lib/postgresql/10/bin/initdb ${PGDATA}

echo "archive_mode = on" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_command = '/usr/bin/timeout 600 /usr/bin/wal-g --config=${TMP_CONFIG} wal-push %p'" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_timeout = 600" >> /var/lib/postgresql/10/main/postgresql.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start

/tmp/scripts/wait_while_pg_not_ready.sh

wal-g --config=${TMP_CONFIG} delete everything FORCE --confirm

# this test checks that wal-g correctly behaves when we are trying to mark existing permanent backup
# as permanent (should not produce any error and exit normally)

# push some impermanent backups (base + 2 deltas)
for i in 1 2 3
do
    pgbench -i -s 1 postgres &
    sleep 1
    wal-g --config=${TMP_CONFIG} backup-push "${PGDATA}"
done

wal-g --config=${TMP_CONFIG} backup-list
# shellcheck disable=SC2006
last_backup_name=`wal-g --config=${TMP_CONFIG} backup-list | tail -n 1 | cut -f 1 -d " "`

# should mark all backups as permanent
wal-g --config=${TMP_CONFIG} backup-mark "$last_backup_name"

# both should be true (is_permanent: true)
first_backup_status=$(wal-g --config=${TMP_CONFIG} backup-list --detail | awk 'NR==2 {print $1}' | egrep -o -e "true" -e "false")
last_backup_status=$(wal-g --config=${TMP_CONFIG} backup-list --detail | awk 'END {print $1}' | egrep -o -e "true" -e "false")

if [ $first_backup_status != $last_backup_status ];
then
    echo "Wrong backup status"
    exit 2
fi

# mark the backup as permanent again, should not cause any error
wal-g --config=${TMP_CONFIG} backup-mark "$last_backup_name"

# push a new permanent delta backup, should not produce any error
pgbench -i -s 1 postgres &
sleep 1
wal-g --config=${TMP_CONFIG} backup-push "${PGDATA}" --permanent

# save backup-list output before delete
lines_before_delete=`wal-g --config=${TMP_CONFIG} backup-list | wc -l`
wal-g --config=${TMP_CONFIG} backup-list > /tmp/list_before_delete

# try to delete all backups
last_backup_name=$(wal-g --config=${TMP_CONFIG} backup-list | tail -n 1 | cut -f 1 -d " ")
wal-g --config=${TMP_CONFIG} delete before "$last_backup_name" --confirm
wal-g --config=${TMP_CONFIG} backup-list

lines_after_delete=`wal-g --config=${TMP_CONFIG} backup-list | wc -l`
wal-g --config=${TMP_CONFIG} backup-list > /tmp/list_after_delete

if [ $(($lines_before_delete)) -ne $lines_after_delete ];
then
    echo $(($lines_before_delete)) > /tmp/before_delete
    echo $lines_after_delete > /tmp/after_delete
    echo "Wrong number of deleted lines"
    diff /tmp/before_delete /tmp/after_delete
fi

/tmp/scripts/drop_pg.sh
