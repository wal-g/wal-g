#!/bin/sh
set -e -x
CONFIG_FILE="/tmp/configs/transfer_backup_test_config.json"
FAILOVER_CONFIG_FILE="/tmp/configs/transfer_backup_test_config_failover.json"
COMMON_CONFIG="/tmp/configs/common_config.json"
TMP_CONFIG="/tmp/configs/tmp_config.json"
FAILOVER_TMP_CONFIG="/tmp/configs/failover_tmp_config.json"
cat ${CONFIG_FILE} > ${TMP_CONFIG}
echo "," >> ${TMP_CONFIG}
cat ${COMMON_CONFIG} >> ${TMP_CONFIG}
/tmp/scripts/wrap_config_file.sh ${TMP_CONFIG}

cat ${FAILOVER_CONFIG_FILE} > ${FAILOVER_TMP_CONFIG}
echo "," >> ${FAILOVER_TMP_CONFIG}
cat ${COMMON_CONFIG} >> ${FAILOVER_TMP_CONFIG}
/tmp/scripts/wrap_config_file.sh ${FAILOVER_TMP_CONFIG}

/usr/lib/postgresql/10/bin/initdb ${PGDATA}

echo "archive_mode = on" >> ${PGDATA}/postgresql.conf
echo "archive_command = '/usr/bin/timeout 600 wal-g --config=${FAILOVER_TMP_CONFIG} wal-push %p'" >> ${PGDATA}/postgresql.conf
echo "archive_timeout = 600" >> ${PGDATA}/postgresql.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start

/tmp/scripts/wait_while_pg_not_ready.sh

wal-g --config=${TMP_CONFIG} st rm / --target=all || true

pgbench -i -s 5 postgres
pg_dumpall -f /tmp/dump1
pgbench -c 2 -T 100000000 -S &
sleep 1
wal-g --config=${FAILOVER_TMP_CONFIG} backup-push ${PGDATA}
/tmp/scripts/drop_pg.sh

wal-g --config=${TMP_CONFIG} st transfer backups --source=failover --target=default --overwrite --fail-fast
wal-g --config=${TMP_CONFIG} st transfer pg-wals --source=failover --target=default --overwrite --fail-fast

wal-g --config=${TMP_CONFIG} backup-fetch ${PGDATA} LATEST

echo "restore_command = 'echo \"WAL file restoration: %f, %p\"&& wal-g --config=${TMP_CONFIG} wal-fetch \"%f\" \"%p\"'" > ${PGDATA}/recovery.conf

wal-g --config=${TMP_CONFIG} st ls -r --target failover
wal-g --config=${TMP_CONFIG} st ls -r --target default

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start
/tmp/scripts/wait_while_pg_not_ready.sh
pg_dumpall -f /tmp/dump2

diff /tmp/dump1 /tmp/dump2

psql -f /tmp/scripts/amcheck.sql -v "ON_ERROR_STOP=1" postgres

/tmp/scripts/drop_pg.sh
rm $TMP_CONFIG
rm $FAILOVER_TMP_CONFIG

echo "Full backup success!!!!!!"
