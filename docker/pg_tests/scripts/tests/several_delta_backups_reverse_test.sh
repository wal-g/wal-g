#!/bin/sh
set -e -x

. /tmp/tests/test_functions/prepare_config.sh
prepare_config "/tmp/configs/several_delta_backups_reverse_test_config.json"

initdb ${PGDATA}

echo "archive_mode = on" >> ${PGDATA}/postgresql.conf
echo "archive_command = '/usr/bin/timeout 600 /usr/bin/wal-g --config=${TMP_CONFIG} wal-push %p && mkdir -p /tmp/deltas/$(basename %p)'" >> ${PGDATA}/postgresql.conf
echo "archive_timeout = 600" >> ${PGDATA}/postgresql.conf

pg_ctl -D ${PGDATA} -w start

wal-g --config=${TMP_CONFIG} delete everything FORCE --confirm

pgbench -i -s 5 postgres
pgbench -T 100000000 postgres &
wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}

wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}

psql -f /tmp/scripts/amcheck.sql -v "ON_ERROR_STOP=1" postgres

wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}

wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}

pkill pgbench

pg_ctl -D ${PGDATA} -m smart -w stop
pg_ctl -D ${PGDATA} -w -t 100500 start

pg_dumpall -f /tmp/dump1
sleep 1
/tmp/scripts/drop_pg.sh

wal-g --config=${TMP_CONFIG} backup-fetch ${PGDATA} LATEST --reverse-unpack

echo "restore_command = 'echo \"WAL file restoration: %f, %p\"&& /usr/bin/wal-g --config=${TMP_CONFIG} wal-fetch \"%f\" \"%p\"'" > ${PGDATA}/recovery.conf

pg_ctl -D ${PGDATA} -w -t 100500 start
sleep 10


pg_dumpall -f /tmp/dump2

psql -f /tmp/scripts/amcheck.sql -v "ON_ERROR_STOP=1" postgres

diff /tmp/dump1 /tmp/dump2
/tmp/scripts/drop_pg.sh
rm ${TMP_CONFIG}
