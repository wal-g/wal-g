#!/bin/sh
set -e -x

export WALE_FILE_PREFIX=file://localhost/tmp
export WALE_LOG_DESTINATION=stderr

WAL_PUSH_LOGS="/tmp/logs/wal_push_logs/pg_wale_compatibility_test_logs"
WAL_FETCH_LOGS="/tmp/logs/wal_fetch_logs/pg_wale_compatibility_test_logs"
BACKUP_PUSH_LOGS="/tmp/logs/backup_push_logs/pg_wale_compatibility_test_logs"
BACKUP_FETCH_LOGS="/tmp/logs/backup_fetch_logs/pg_wale_compatibility_test_logs"

/usr/lib/postgresql/10/bin/initdb ${PGDATA}

echo "archive_mode = on" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_command = '/usr/bin/timeout 600 wal-e wal-push %p'" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_timeout = 600" >> /var/lib/postgresql/10/main/postgresql.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start

pgbench -i -s 20 postgres
pg_dumpall -f /tmp/dump1
pgbench -c 2 -T 100000000 -S &
sleep 1
wal-e backup-push ${PGDATA}

pkill -9 postgres

mkdir /tmp/conf_files
cp ${PGDATA}/postgresql.conf /tmp/conf_files
cp ${PGDATA}/pg_hba.conf /tmp/conf_files
cp ${PGDATA}/pg_ident.conf /tmp/conf_files

rm -rf ${PGDATA}

/usr/bin/time -v -a --output ${BACKUP_FETCH_LOGS} wal-g backup-fetch ${PGDATA} LATEST

echo "restore_command = 'echo \"WAL file restoration: %f, %p\"&& /usr/bin/time -v -a --output ${WAL_FETCH_LOGS} /usr/bin/wal-g wal-fetch \"%f\" \"%p\"'" > ${PGDATA}/recovery.conf

cp /tmp/conf_files/postgresql.conf ${PGDATA}
cp /tmp/conf_files/pg_hba.conf ${PGDATA}
cp /tmp/conf_files/pg_ident.conf ${PGDATA}

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start

pg_dumpall -f /tmp/dump2

diff /tmp/dump1 /tmp/dump2

tmp/scripts/drop_pg.sh

echo "WAL-E compatible backup-fetch success!!!!!!"
