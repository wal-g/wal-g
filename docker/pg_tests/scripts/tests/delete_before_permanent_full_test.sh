#!/bin/sh
set -e -x


/usr/lib/postgresql/10/bin/initdb ${PGDATA}

echo "archive_mode = on" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_command = '/usr/bin/timeout 600 /usr/bin/wal-g --config=/tmp/configs/delete_before_permanent_full_test_config.json wal-push %p'" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_timeout = 600" >> /var/lib/postgresql/10/main/postgresql.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start

# push first backup as permanent
pgbench -i -s 1 postgres &
sleep 1
wal-g --config=/tmp/configs/delete_before_permanent_full_test_config.json backup-push --permanent ${PGDATA}
wal-g --config=/tmp/configs/delete_before_permanent_full_test_config.json backup-list
permanent_backup_name=`wal-g --config=/tmp/configs/delete_before_permanent_full_test_config.json backup-list | tail -n 1 | cut -f 1 -d " "`

# push a few more impermanent backups
for i in 2 3 4
do
    pgbench -i -s 1 postgres &
    sleep 1
    wal-g --config=/tmp/configs/delete_before_permanent_full_test_config.json backup-push ${PGDATA}
done
wal-g --config=/tmp/configs/delete_before_permanent_full_test_config.json backup-list

# delete all backups
last_backup_name=`wal-g --config=/tmp/configs/delete_before_permanent_full_test_config.json backup-list | tail -n 1 | cut -f 1 -d " "`
wal-g --config=/tmp/configs/delete_before_permanent_full_test_config.json delete before $last_backup_name --confirm
wal-g --config=/tmp/configs/delete_before_permanent_full_test_config.json backup-list

# check that permanent backup still exists
first_backup_name=`wal-g --config=/tmp/configs/delete_before_permanent_full_test_config.json backup-list | sed '2q;d' | cut -f 1 -d " "`
if [ $first_backup_name != $permanent_backup_name ];
then
    echo "permanent backup does not exist after deletion"
fi

scripts/drop_pg.sh

echo "Delete before permanent full success!!!!!!"
