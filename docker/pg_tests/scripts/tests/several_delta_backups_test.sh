#!/bin/sh
set -e -x

export WALE_S3_PREFIX=s3://compressionbucket
export WALG_USE_WAL_DELTA=true

/usr/lib/postgresql/10/bin/initdb ${PGDATA}

echo "archive_mode = on" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_command = '/usr/bin/timeout 600 /usr/bin/wal-g wal-push %p && mkdir -p /tmp/deltas/$(basename %p)'" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_timeout = 600" >> /var/lib/postgresql/10/main/postgresql.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start

pgbench -i -s 10 postgres
pgbench -T 100000000 postgres &
wal-g backup-push ${PGDATA}

export WALG_COMPRESSION_METHOD=lz4
wal-g backup-push ${PGDATA}

export WALG_COMPRESSION_METHOD=brotli
wal-g backup-push ${PGDATA}

export WALG_COMPRESSION_METHOD=brotli
pkill pgbench
pg_dumpall -f /tmp/dump1
sleep 1

scripts/drop_pg.sh

wal-g backup-fetch ${PGDATA} LATEST

echo "restore_command = 'echo \"WAL file restoration: %f, %p\"&& /usr/bin/wal-g wal-fetch \"%f\" \"%p\"'" > ${PGDATA}/recovery.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start

pg_dumpall -f /tmp/dump2

diff /tmp/dump1 /tmp/dump2

psql -f scripts/amcheck.sql postgres

scripts/drop_pg.sh

echo "Several delta backup success!!!!!!"
