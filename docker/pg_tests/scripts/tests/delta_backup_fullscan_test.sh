#!/bin/sh
set -e -x


/usr/lib/postgresql/10/bin/initdb ${PGDATA}

echo "archive_mode = on" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_command = '/usr/bin/timeout 600 /usr/bin/wal-g --config=/tmp/configs/delta_backup_fullscan_test_config.json wal-push %p'" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_timeout = 600" >> /var/lib/postgresql/10/main/postgresql.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start

pgbench -i -s 10 postgres
wal-g --config=/tmp/configs/delta_backup_fullscan_test_config.json backup-push ${PGDATA}

pgbench -i -s 20 postgres
pg_dumpall -f /tmp/dump1
pgbench -c 2 -T 100000000 -S &
sleep 1
wal-g --config=/tmp/configs/delta_backup_fullscan_test_config.json backup-push ${PGDATA}

tmp/scripts/drop_pg.sh

wal-g --config=/tmp/configs/delta_backup_fullscan_test_config.json backup-fetch ${PGDATA} LATEST

echo "restore_command = 'echo \"WAL file restoration: %f, %p\"&& /usr/bin/wal-g --config=/tmp/configs/delta_backup_fullscan_test_config.json wal-fetch \"%f\" \"%p\"'" > ${PGDATA}/recovery.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start

pg_dumpall -f /tmp/dump2

diff /tmp/dump1 /tmp/dump2

psql -f tmp/scripts/amcheck.sql postgres
tmp/scripts/drop_pg.sh

echo "Fullscan delta backup success!!!!!!"
