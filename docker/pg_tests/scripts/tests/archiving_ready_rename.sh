#!/bin/sh
set -e -x
CONFIG_FILE="/tmp/configs/archiving_ready_rename.json"
COMMON_CONFIG="/tmp/configs/common_config.json"
TMP_CONFIG="/tmp/configs/tmp_config.json"
export PGDATA="/var/lib/postgresql/10/main"
cat ${CONFIG_FILE} > ${TMP_CONFIG}
echo "," >> ${TMP_CONFIG}
cat ${COMMON_CONFIG} >> ${TMP_CONFIG}
/tmp/scripts/wrap_config_file.sh ${TMP_CONFIG}

wal-g delete everything FORCE --confirm --config=${TMP_CONFIG}

/usr/lib/postgresql/10/bin/initdb ${PGDATA}

echo "archive_mode = on" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_timeout = 600" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_command = 'exit 1'" > /var/lib/postgresql/10/main/postgresql.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start

pgbench -i -s 20 postgres

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w stop

echo "logging_collector = on" >> /var/lib/postgresql/10/main/postgresql.conf
echo "log_filename = 'postgresql.log'" >> /var/lib/postgresql/10/main/postgresql.conf
echo "log_min_messages = debug" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_command = '/usr/bin/wal-g --config=${TMP_CONFIG} wal-push %p --pg-ready-rename=true'" > /var/lib/postgresql/10/main/postgresql.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start
/tmp/scripts/wait_while_pg_not_ready.sh

sleep 10

count=$(grep -c 'archived write-ahead log file' /var/lib/postgresql/10/main/log/postgresql.log)

if [ "${count}" = '0' ]; then
    exit 1
fi

/tmp/scripts/drop_pg.sh
