#!/bin/sh
set -e -x
CONFIG_FILE="/tmp/configs/several_delta_backups_reverse_test_config.json"
COMMON_CONFIG="/tmp/configs/common_config.json"
TMP_CONFIG="/tmp/configs/tmp_config.json"
cat ${CONFIG_FILE} > ${TMP_CONFIG}
echo "," >> ${TMP_CONFIG}
cat ${COMMON_CONFIG} >> ${TMP_CONFIG}
/tmp/scripts/wrap_config_file.sh ${TMP_CONFIG}

/usr/lib/postgresql/10/bin/initdb ${PGDATA}

echo "archive_mode = on" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_command = '/usr/bin/timeout 600 /usr/bin/wal-g --config=${TMP_CONFIG} wal-push %p && mkdir -p /tmp/deltas/$(basename %p)'" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_timeout = 600" >> /var/lib/postgresql/10/main/postgresql.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start

/tmp/scripts/wait_while_pg_not_ready.sh

wal-g --config=${TMP_CONFIG} delete everything FORCE --confirm

pgbench -i -s 5 postgres
pgbench -T 100000000 postgres &
wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}

wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}

psql -f /tmp/scripts/amcheck.sql -v "ON_ERROR_STOP=1" postgres

wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}

wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}

pkill pgbench

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -m smart -w stop
/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w -t 100500 start

pg_dumpall -f /tmp/dump1
sleep 1
/tmp/scripts/drop_pg.sh

wal-g --config=${TMP_CONFIG} backup-fetch ${PGDATA} LATEST --reverse-unpack

echo "restore_command = 'echo \"WAL file restoration: %f, %p\"&& /usr/bin/wal-g --config=${TMP_CONFIG} wal-fetch \"%f\" \"%p\"'" > ${PGDATA}/recovery.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w -t 100500 start
sleep 10


pg_dumpall -f /tmp/dump2

psql -f /tmp/scripts/amcheck.sql -v "ON_ERROR_STOP=1" postgres

diff /tmp/dump1 /tmp/dump2
/tmp/scripts/drop_pg.sh
rm ${TMP_CONFIG}
