#!/bin/sh
set -e -x

mkdir /tmp/storage

. /tmp/tests/test_functions/prepare_config.sh
prepare_config "/tmp/configs/config_test_config.json"

initdb ${PGDATA}

echo "archive_mode = on" >> ${PGDATA}/postgresql.conf
echo "archive_command = '/usr/bin/timeout 600 /usr/bin/wal-g --config=${TMP_CONFIG} wal-push %p'" >> ${PGDATA}/postgresql.conf
echo "archive_timeout = 600" >> ${PGDATA}/postgresql.conf

pg_ctl -D ${PGDATA} -w start

wal-g --config=${TMP_CONFIG} delete everything FORCE --confirm

pgbench -i -s 4 postgres
pg_dumpall -f /tmp/dump1
pgbench -c 2 -T 100000000 -S &
sleep 1
wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}

pkill -9 postgres

rm -rf "${PGDATA}"

wal-g --config=${TMP_CONFIG} --turbo backup-fetch "${PGDATA}" LATEST

echo "restore_command = 'echo \"WAL file restoration: %f, %p\"&& /usr/bin/wal-g --config=${TMP_CONFIG} wal-fetch \"%f\" \"%p\"'" > "${PGDATA}"/recovery.conf

pg_ctl -D "${PGDATA}" -w start

/tmp/scripts/wait_while_pg_not_ready.sh

pg_dumpall -f /tmp/dump2

diff /tmp/dump1 /tmp/dump2

pkill -9 postgres
rm -rf "${PGDATA}"
rm ${TMP_CONFIG}

/tmp/scripts/drop_pg.sh
