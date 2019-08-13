#!/bin/sh
set -e -x

CONFIG_FILE="/tmp/configs/delta_backup_wal_delta_test_config.json"
COMMON_CONFIG="/tmp/configs/common_config.json"
TMP_CONFIG="/tmp/configs/tmp_config.json"
cat ${CONFIG_FILE} > ${TMP_CONFIG}
echo "," >> ${TMP_CONFIG}
cat ${COMMON_CONFIG} >> ${TMP_CONFIG}

tmp/scripts/wrap_config_file.sh ${TMP_CONFIG}


WAL_PUSH_LOGS="/tmp/logs/wal_push_logs/pg_delta_backup_wal_delta_test_logs"
WAL_FETCH_LOGS="/tmp/logs/wal_fetch_logs/pg_delta_backup_wal_delta_test_logs"
BACKUP_PUSH_LOGS="/tmp/logs/backup_push_logs/pg_delta_backup_wal_delta_test_logs"
BACKUP_FETCH_LOGS="/tmp/logs/backup_fetch_logs/pg_delta_backup_wal_delta_test_logs"

/usr/lib/postgresql/10/bin/initdb ${PGDATA}

echo "archive_mode = on" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_command = '/usr/bin/timeout 600 /usr/bin/time -v -a --output ${WAL_PUSH_LOGS} /usr/bin/wal-g --config=${TMP_CONFIG} wal-push %p && mkdir -p /tmp/deltas/$(basename %p)'" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_timeout = 600" >> /var/lib/postgresql/10/main/postgresql.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start

pgbench -i -s 30 postgres
/usr/bin/time -v -a --output ${BACKUP_PUSH_LOGS} wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}

pgbench -i -s 40 postgres
pg_dumpall -f /tmp/dump1
pgbench -c 2 -T 100000000 -S &
sleep 1
/usr/bin/time -v -a --output ${BACKUP_PUSH_LOGS} wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}

tmp/scripts/drop_pg.sh

/usr/bin/time -v -a --output ${BACKUP_FETCH_LOGS} wal-g --config=${TMP_CONFIG} backup-fetch ${PGDATA} LATEST

echo "restore_command = 'echo \"WAL file restoration: %f, %p\"&& /usr/bin/time -v -a --output ${WAL_FETCH_LOGS} /usr/bin/wal-g --config=${TMP_CONFIG} wal-fetch \"%f\" \"%p\"'" > ${PGDATA}/recovery.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start

pg_dumpall -f /tmp/dump2

diff /tmp/dump1 /tmp/dump2

psql -f tmp/scripts/amcheck.sql -v "ON_ERROR_STOP=1" postgres
tmp/scripts/drop_pg.sh
rm ${TMP_CONFIG}
echo "Wal delta backup success!!!!!!"
