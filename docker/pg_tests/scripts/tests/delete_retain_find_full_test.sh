#!/bin/sh
set -e -x
CONFIG_FILE="/tmp/configs/delete_retain_find_full_test_config.json"

COMMON_CONFIG="/tmp/configs/common_config.json"
echo "," >> ${CONFIG_FILE}
cat ${COMMON_CONFIG} >> ${CONFIG_FILE}

/usr/lib/postgresql/10/bin/initdb ${PGDATA}

echo "archive_mode = on" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_command = '/usr/bin/timeout 600 /usr/bin/wal-g --config=${CONFIG_FILE} wal-push %p'" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_timeout = 600" >> /var/lib/postgresql/10/main/postgresql.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start

for i in 1 2 3 4 5 6
do
    pgbench -i -s 1 postgres &
    sleep 1
    wal-g --config=${CONFIG_FILE} backup-push ${PGDATA}
done

wal-g --config=${CONFIG_FILE} backup-list
lines_before_delete=`wal-g --config=${CONFIG_FILE} backup-list | wc -l`
wal-g --config=${CONFIG_FILE} backup-list | tail -n 4 > /tmp/list_tail_before_delete

wal-g --config=${CONFIG_FILE} delete retain FIND_FULL 3 --confirm

wal-g --config=${CONFIG_FILE} backup-list
lines_after_delete=`wal-g --config=${CONFIG_FILE} backup-list | wc -l`
wal-g --config=${CONFIG_FILE} backup-list | tail -n 4 > /tmp/list_tail_after_delete

if [ $(($lines_before_delete-2)) -ne $lines_after_delete ];
then
    echo $(($lines_before_delete-2)) > /tmp/before_delete
    echo $lines_after_delete > /tmp/after_delete
    echo "Wrong number of deleted lines"
    diff /tmp/before_delete /tmp/after_delete
fi

diff /tmp/list_tail_before_delete /tmp/list_tail_after_delete

tmp/scripts/drop_pg.sh

echo "Delete retain FIND_FULL success!!!!!!"
