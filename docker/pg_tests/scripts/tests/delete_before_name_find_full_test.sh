#!/bin/sh
set -e -x

export WALE_S3_PREFIX=s3://deletebeforenamefindfullbucket
export WALG_USE_WAL_DELTA=true
export WALG_DELTA_MAX_STEPS=1

/usr/lib/postgresql/10/bin/initdb ${PGDATA}

echo "archive_mode = on" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_command = '/usr/bin/timeout 600 /usr/bin/wal-g wal-push %p'" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_timeout = 600" >> /var/lib/postgresql/10/main/postgresql.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start

for i in 1 2 3 4
do
    pgbench -i -s 1 postgres &
    sleep 1
    wal-g backup-push ${PGDATA}
done

# take name of last backup(it's delta)
backup_name=`wal-g backup-list | tail -n 1 | cut -f 1 -d " "`

wal-g backup-list
lines_before_delete=`wal-g backup-list | wc -l`
wal-g backup-list | tail -n 2 > /tmp/list_tail_before_delete

wal-g delete before FIND_FULL $backup_name --confirm

wal-g backup-list
lines_after_delete=`wal-g backup-list | wc -l`
wal-g backup-list | tail -n 2 > /tmp/list_tail_after_delete

if [ $(($lines_before_delete-2)) -ne $lines_after_delete ];
then
    echo $(($lines_before_delete-2)) > /tmp/before_delete
    echo $lines_after_delete > /tmp/after_delete
    echo "Wrong number of deleted lines"
    diff /tmp/before_delete /tmp/after_delete
fi

diff /tmp/list_tail_before_delete /tmp/list_tail_after_delete

echo "____ls____"
ls
echo "____ls tmp____"
ls tmp
echo "____ls tmp/scripts____"
ls tmp/scripts


tmp/scripts/drop_pg.sh

echo "Delete before FIND_FULL name success!!!!!!"
