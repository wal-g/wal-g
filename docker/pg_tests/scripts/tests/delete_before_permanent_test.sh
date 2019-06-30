#!/bin/sh
set -e -x

export WALE_S3_PREFIX=s3://deletebeforenamefindfullbucket
export WALG_USE_WAL_DELTA=true
export WALG_DELTA_MAX_STEPS=0

/usr/lib/postgresql/10/bin/initdb ${PGDATA}

echo "archive_mode = on" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_command = '/usr/bin/timeout 600 /usr/bin/wal-g wal-push %p'" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_timeout = 600" >> /var/lib/postgresql/10/main/postgresql.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start

for i in 1
do
    pgbench -i -s 1 postgres &
    sleep 1
    wal-g backup-push --permanent ${PGDATA}
done

echo "backup list after pushing permanent backups:"
wal-g backup-list
permanent_backup_name=`wal-g backup-list | tail -n 1 | cut -f 1 -d " "`

for i in 2 3 4
do
    pgbench -i -s 1 postgres &
    sleep 1
    wal-g backup-push ${PGDATA}
done

echo "backup list after pushing all backups:"
wal-g backup-list

last_backup_name=`wal-g backup-list | tail -n 1 | cut -f 1 -d " "`
wal-g delete before FIND_FULL $last_backup_name --confirm

echo "backup list after delete:"
wal-g backup-list

# retrieve name of first backup
first_backup_name=`wal-g backup-list | sed '2q;d' | cut -f 1 -d " "`

if [ $first_backup_name -ne $permanent_backup_name ];
then
    echo "permanent backup does not exist after deletion"
fi

scripts/drop_pg.sh

echo "Delete before time permanent success!!!!!!"
