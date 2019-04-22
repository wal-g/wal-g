#!/bin/sh
set -e -x

export WALE_S3_PREFIX=s3://deleteendtoendbucket
export WALG_USE_WAL_DELTA=true
export WALG_DELTA_MAX_STEPS=2

/usr/lib/postgresql/10/bin/initdb ${PGDATA}

echo "archive_mode = on" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_command = '/usr/bin/timeout 600 /usr/bin/wal-g wal-push %p'" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_timeout = 600" >> /var/lib/postgresql/10/main/postgresql.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start

for i in $(seq 1 9);
do
    pgbench -i -s 2 postgres
    if [ $i -eq 4 -o $i -eq 9 ];
    then
        pg_dumpall -f /tmp/dump$i
    fi
    pgbench -c 2 -T 100000000 -S &
    sleep 1
    wal-g backup-push ${PGDATA}
done

wal-g backup-list

target_backup_name=`wal-g backup-list | tail -n 6 | head -n 1 | cut -f 1 -d " "`

wal-g delete before FIND_FULL $target_backup_name --confirm

wal-g backup-list

FIRST=`wal-g backup-list | head -n 2 | tail -n 1 | cut -f 1 -d " "`

for i in ${FIRST} LATEST
do
    pkill -9 postgres
    rm -rf ${PGDATA}
    wal-g backup-fetch ${PGDATA} ${i}
    echo "restore_command = 'echo \"WAL file restoration: %f, %p\"&& /usr/bin/wal-g wal-fetch \"%f\" \"%p\"'" > ${PGDATA}/recovery.conf
    /usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start
    wal-g backup-list
    sleep 10
    pg_dumpall -f /tmp/dump${i}
done

diff /tmp/dump4 /tmp/dump${FIRST}
diff /tmp/dump9 /tmp/dumpLATEST

pkill -9 postgres
rm -rf ${PGDATA}

echo $target_backup_name
echo $FIRST
echo "End to end delete test success!!!!!!"
