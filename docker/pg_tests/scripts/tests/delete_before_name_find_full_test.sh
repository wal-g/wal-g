#!/bin/sh
set -e -x
CONFIG_FILE="/tmp/configs/delete_before_name_find_full_test_config.json"

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

wal-g --config=${TMP_CONFIG} delete everything FORCE --confirm

for i in 1 2 3 4
do
    pgbench -i -s 1 postgres &
    sleep 1
    wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}
done

# take name of last backup(it's delta)
backup_name=`wal-g --config=${TMP_CONFIG} backup-list | tail -n 1 | cut -f 1 -d " "`

wal-g --config=${TMP_CONFIG} backup-list
lines_before_delete=`wal-g --config=${TMP_CONFIG} backup-list | wc -l`
wal-g --config=${TMP_CONFIG} backup-list | tail -n 2 > /tmp/list_tail_before_delete

wal-g --config=${TMP_CONFIG} delete before FIND_FULL $backup_name --confirm

wal-g --config=${TMP_CONFIG} backup-list
lines_after_delete=`wal-g --config=${TMP_CONFIG} backup-list | wc -l`
wal-g --config=${TMP_CONFIG} backup-list | tail -n 2 > /tmp/list_tail_after_delete

if [ $(($lines_before_delete-2)) -ne $lines_after_delete ];
then
    echo $(($lines_before_delete-2)) > /tmp/before_delete
    echo $lines_after_delete > /tmp/after_delete
    echo "Wrong number of deleted lines"
    diff /tmp/before_delete /tmp/after_delete
fi

diff /tmp/list_tail_before_delete /tmp/list_tail_after_delete
/tmp/scripts/drop_pg.sh
rm ${TMP_CONFIG}
echo "Delete before FIND_FULL name success!!!!!!"
