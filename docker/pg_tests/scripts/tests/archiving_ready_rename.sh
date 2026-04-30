#!/bin/sh
set -e -x

. /tmp/tests/test_functions/prepare_config.sh
prepare_config "/tmp/configs/archiving_ready_rename.json"

wal-g delete everything FORCE --confirm --config=${TMP_CONFIG}

initdb ${PGDATA}

echo "archive_mode = on" >> ${PGDATA}/postgresql.conf
echo "archive_timeout = 600" >> ${PGDATA}/postgresql.conf
echo "archive_command = 'exit 1'" > ${PGDATA}/postgresql.auto.conf

pg_ctl -D ${PGDATA} -w start

pgbench -i -s 20 postgres

pg_ctl -D ${PGDATA} -w stop

echo "logging_collector = on" >> ${PGDATA}/postgresql.conf
echo "log_filename = 'postgresql.log'" >> ${PGDATA}/postgresql.conf
echo "log_min_messages = debug" >> ${PGDATA}/postgresql.conf
echo "archive_command = '/usr/bin/timeout 600 /usr/bin/wal-g --config=${TMP_CONFIG} wal-push %p --pg-ready-rename=true'" > ${PGDATA}/postgresql.auto.conf

pg_ctl -D ${PGDATA} -w start
/tmp/scripts/wait_while_pg_not_ready.sh

sleep 10

grep -i 'archived write-ahead log file' ${PGDATA}/log/postgresql.log

count=$(grep -c 'archived write-ahead log file' ${PGDATA}/log/postgresql.log)

if [ "${count}" = '0' ]; then
    exit 1
fi

/tmp/scripts/drop_pg.sh
