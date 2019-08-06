#!/bin/sh
set -e -x

/usr/lib/postgresql/10/bin/initdb ${PGDATA}

echo "archive_mode = on" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_command = '/usr/bin/timeout 600 /usr/bin/wal-g wal-push %p'" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_timeout = 600" >> /var/lib/postgresql/10/main/postgresql.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start

# push permanent and impermanent delta backups
for i in 1 2 3 4
do
    pgbench -i -s 1 postgres &
    sleep 1
    if [ $i -e 3 ]
    then
        wal-g backup-push --permanent ${PGDATA}
        pg_dumpall -f /tmp/dump1
    else
        wal-g backup-push ${PGDATA}
    fi
done
wal-g backup-list

# delete backups by pushing a full backup and running `delete retain 1`
# this should only delete the last impermanent delta backup
pgbench -i -s 1 postgres &
sleep 1
wal-g backup-push ${PGDATA}
wal-g delete retain 1
wal-g backup-list

# restore the backup and compare with previous state
scripts/drop_pg.sh
first_backup_name=`wal-g backup-list | sed '2q;d' | cut -f 1 -d " "`
wal-g backup-fetch ${PGDATA} $first_backup_name
echo "restore_command = 'echo \"WAL file restoration: %f, %p\"&& /usr/bin/wal-g wal-fetch \"%f\" \"%p\"'" > ${PGDATA}/recovery.conf
/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start
pg_dumpall -f /tmp/dump2
diff /tmp/dump1 /tmp/dump2

scripts/drop_pg.sh

echo "Delete before permanent delta success!!!!!!"