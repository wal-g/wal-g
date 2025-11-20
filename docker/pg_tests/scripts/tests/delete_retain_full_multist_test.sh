#!/bin/sh
set -e -x

. /tmp/tests/test_functions/prepare_config.sh
prepare_config "/tmp/configs/delete_retain_full_multist_test_config.json"

initdb ${PGDATA}

echo "archive_mode = on" >> ${PGDATA}/postgresql.conf
echo "archive_command = '/usr/bin/timeout 600 /usr/bin/wal-g --config=${TMP_CONFIG} wal-push %p'" >> ${PGDATA}/postgresql.conf
echo "archive_timeout = 600" >> ${PGDATA}/postgresql.conf

pg_ctl -D ${PGDATA} -w start
wal-g --config=${TMP_CONFIG} delete everything FORCE --confirm

for _ in 1 2 3 4
do
    pgbench -i -s 1 postgres &
    sleep 1
    wal-g --config=${TMP_CONFIG} backup-push ${PGDATA} --target-storage default
done

# copy all backups to the failover storage
wal-g --config=${TMP_CONFIG} st transfer backups --source default --target good_failover --preserve

wal-g --config=${TMP_CONFIG} backup-list
lines_before_delete=`wal-g --config=${TMP_CONFIG} backup-list | wc -l`
wal-g --config=${TMP_CONFIG} backup-list > /tmp/list_before_delete

wal-g --config=${TMP_CONFIG} delete retain FULL 1 --confirm

wal-g --config=${TMP_CONFIG} backup-list
lines_after_delete=`wal-g --config=${TMP_CONFIG} backup-list | wc -l`
wal-g --config=${TMP_CONFIG} backup-list > /tmp/list_after_delete

# we deleted 1 base backup and 1 delta backup from each of 2 storages
expected_backups_deleted=$(((1+1)*2))

if [ $(($lines_before_delete-$expected_backups_deleted)) -ne $lines_after_delete ];
then
    echo $(($lines_before_delete-$expected_backups_deleted)) > /tmp/before_delete
    echo $lines_after_delete > /tmp/after_delete
    echo "Wrong number of deleted lines"
    diff /tmp/before_delete /tmp/after_delete
fi

# ensure all backups which we weren't going to delete still exist after performing deletion
xargs -I {} grep {} /tmp/list_before_delete </tmp/list_after_delete

/tmp/scripts/drop_pg.sh
rm ${TMP_CONFIG}
