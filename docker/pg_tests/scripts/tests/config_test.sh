#!/bin/sh
set -e -x
CONFIG_FILE="/tmp/configs/config_test_config.json"
mkdir /tmp/storage
tmp/scripts/wrap_config_file.sh ${CONFIG_FILE}

/usr/lib/postgresql/10/bin/initdb ${PGDATA}

echo "archive_mode = on" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_command = '/usr/bin/timeout 600 /usr/bin/wal-g --config=${CONFIG_FILE} wal-push %p'" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_timeout = 600" >> /var/lib/postgresql/10/main/postgresql.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start

pgbench -i -s 10 postgres
pg_dumpall -f /tmp/dump1
pgbench -c 2 -T 100000000 -S &
sleep 1
wal-g --config=${CONFIG_FILE} backup-push ${PGDATA}

pkill -9 postgres

rm -rf ${PGDATA}

wal-g --config=${CONFIG_FILE} backup-fetch ${PGDATA} LATEST

echo "restore_command = 'echo \"WAL file restoration: %f, %p\"&& /usr/bin/wal-g --config=${CONFIG_FILE} wal-fetch \"%f\" \"%p\"'" > ${PGDATA}/recovery.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start

pg_dumpall -f /tmp/dump2

diff /tmp/dump1 /tmp/dump2

pkill -9 postgres
rm -rf ${PGDATA}

echo "Full backup success!!!!!!"
