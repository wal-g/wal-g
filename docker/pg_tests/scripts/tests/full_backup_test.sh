#!/bin/sh
exit 0
set -e -x

export WALE_S3_PREFIX=s3://fullbucket
export WALG_PGP_KEY_PATH=/tmp/PGP_KEY

/usr/lib/postgresql/10/bin/initdb ${PGDATA}

echo "archive_mode = on" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_command = '/usr/bin/timeout 600 /usr/bin/wal-g wal-push %p'" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_timeout = 600" >> /var/lib/postgresql/10/main/postgresql.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start

pgbench -i -s 10 postgres
pg_dumpall -f /tmp/dump1
pgbench -c 2 -T 100000000 -S &
sleep 1
wal-g backup-push ${PGDATA}

tmp/scripts/drop_pg.sh

wal-g backup-fetch ${PGDATA} LATEST

echo "restore_command = 'echo \"WAL file restoration: %f, %p\"&& /usr/bin/wal-g wal-fetch \"%f\" \"%p\"'" > ${PGDATA}/recovery.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start

pg_dumpall -f /tmp/dump2

diff /tmp/dump1 /tmp/dump2

psql -f tmp/scripts/amcheck.sql -v "ON_ERROR_STOP=1" postgres
tmp/scripts/drop_pg.sh

echo "Full backup success!!!!!!"

# Also we test here WAL overwrite prevention as a part of regular backup functionality

export WALG_PREVENT_WAL_OVERWRITE=true

echo test > /tmp/test_file

wal-g wal-push /tmp/test_file test_file
wal-g wal-push /tmp/test_file test_file
echo test1 > /tmp/test_file
!wal-g wal-push /tmp/test_file test_file

echo "Prevent WAL overwrite success!!!!!!"