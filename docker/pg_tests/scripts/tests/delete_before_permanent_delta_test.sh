#!/bin/sh
set -e -x

export WALE_S3_PREFIX=s3://deletebeforepermanentdeltabucket
export WALG_USE_WAL_DELTA=true
export WALG_DELTA_MAX_STEPS=3

/usr/lib/postgresql/10/bin/initdb ${PGDATA}

echo "archive_mode = on" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_command = '/usr/bin/timeout 600 /usr/bin/wal-g wal-push %p'" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_timeout = 600" >> /var/lib/postgresql/10/main/postgresql.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start

#delete all backups of any
wal-g delete everything FORCE --confirm

# push permanent and impermanent delta backups
for i in 1 2 3 4
do
    pgbench -i -s 1 postgres &
    sleep 1
    if [ $i -eq 3 ]
    then
        wal-g backup-push --permanent ${PGDATA}
        pg_dumpall -f /tmp/dump1
    else
        wal-g backup-push ${PGDATA}
    fi
done
wal-g backup-list --detail

# delete backups by pushing a full backup and running `delete retain 1`
# this should only delete the last impermanent delta backup
export WALG_DELTA_MAX_STEPS=0
pgbench -i -s 1 postgres &
sleep 1
wal-g backup-push ${PGDATA}

wal-g backup-list --detail

wal-g delete retain 1 --confirm
wal-g backup-list

# restore the backup and compare with previous state
tmp/scripts/drop_pg.sh
first_backup_name=`wal-g backup-list | sed '2q;d' | cut -f 1 -d " "`
wal-g backup-fetch ${PGDATA} $first_backup_name
echo "restore_command = 'echo \"WAL file restoration: %f, %p\"&& /usr/bin/wal-g wal-fetch \"%f\" \"%p\"'" > ${PGDATA}/recovery.conf
/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start
pg_dumpall -f /tmp/dump2
diff /tmp/dump1 /tmp/dump2

wal-g backup-list --detail

# delete all backups after previous tests
wal-g delete everything FORCE --confirm

# make impermanent base backup
wal-g backup-push ${PGDATA}

imperm_backup=`wal-g backup-list | egrep -o "[0-9A-F]{24}"`

# make permanent base backup
wal-g backup-push --permanent ${PGDATA}
wal-g backup-list --detail

# check that nothing changed when permanent backups exist
wal-g backup-list > /tmp/dump1
wal-g delete everything --confirm || true
wal-g backup-list > /tmp/dump2
diff /tmp/dump1 /tmp/dump2

rm /tmp/dump2
touch /tmp/dump2

# delete all backups
wal-g delete everything FORCE --confirm
wal-g backup-list 2> /tmp/2 1> /tmp/1

# check that stdout not include any backup
! cat /tmp/1 | egrep -o "[0-9A-F]{24}" > /tmp/dump1
diff /tmp/dump1 /tmp/dump2

# check that stderr not include any backup
# stderr shuld be "INFO: ... No backups found"
! cat /tmp/2 | egrep -o "[0-9A-F]{24}" > /tmp/dump1
diff /tmp/dump1 /tmp/dump2

tmp/scripts/drop_pg.sh

echo "Delete before permanent delta success!!!!!!"
