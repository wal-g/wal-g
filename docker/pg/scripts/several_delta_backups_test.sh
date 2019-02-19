#!/bin/sh
set -e -x

export WALE_S3_PREFIX=s3://waldeltabucket
export WALG_USE_WAL_DELTA=true

/usr/lib/postgresql/10/bin/initdb ${PGDATA}

echo "archive_mode = on" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_command = '/usr/bin/timeout 600 /usr/bin/wal-g wal-push %p && mkdir -p /tmp/deltas/$(basename %p)'" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_timeout = 600" >> /var/lib/postgresql/10/main/postgresql.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start

pgbench -i -s 10 postgres
wal-g backup-push ${PGDATA}

pgbench -i -s 20 postgres
wal-g backup-push ${PGDATA}
pgbench -i -s 30 postgres
wal-g backup-push ${PGDATA}
pgbench -i -s 40 postgres
pg_dumpall -f /tmp/dump1
sleep 1
wal-g backup-push ${PGDATA}

pkill -9 postgres

rm -rf ${PGDATA}

wal-g backup-fetch ${PGDATA} LATEST

echo "restore_command = 'echo \"WAL file restoration: %f, %p\"&& /usr/bin/wal-g wal-fetch \"%f\" \"%p\"'" > ${PGDATA}/recovery.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start

pg_dumpall -f /tmp/dump2

diff /tmp/dump1 /tmp/dump2

pkill -9 postgres
rm -rf ${PGDATA}

echo "Several delta backup success!!!!!!"
