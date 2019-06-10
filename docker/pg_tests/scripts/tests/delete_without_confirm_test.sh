#!/bin/sh
set -e -x

export WALE_S3_PREFIX=s3://deletewithoutconfirm
export WALG_USE_WAL_DELTA=true
export WALG_DELTA_MAX_STEPS=0

/usr/lib/postgresql/10/bin/initdb ${PGDATA}

echo "archive_mode = on" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_command = '/usr/bin/timeout 600 /usr/bin/wal-g wal-push %p'" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_timeout = 600" >> /var/lib/postgresql/10/main/postgresql.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start

for i in 1 2
do
    pgbench -i -s 1 postgres &
    sleep 1
    wal-g backup-push ${PGDATA}
done

lines_before_delete=`wal-g backup-list | wc -l`
wal-g backup-list > /tmp/list_before_delete

wal-g delete retain FULL 1

lines_after_delete=`wal-g backup-list | wc -l`
wal-g backup-list > /tmp/list_after_delete

if [ $lines_before_delete -ne $lines_after_delete ];
then
    echo $lines_before_delete > /tmp/before_delete
    echo $lines_after_delete > /tmp/after_delete
    echo "Backups were deleted without --confirm"
    diff /tmp/before_delete /tmp/after_delete
fi

diff /tmp/list_before_delete /tmp/list_after_delete

scripts/drop_pg.sh

echo "Delete retain FULL success!!!!!!"
