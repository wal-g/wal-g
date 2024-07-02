#!/bin/sh

set -e -x
CONFIG_FILE="/tmp/configs/compatibility_test_config.json"
COMMON_CONFIG="/tmp/configs/common_config.json"
TMP_CONFIG="/tmp/configs/tmp_config.json"
cat ${CONFIG_FILE} > ${TMP_CONFIG}
echo "," >> ${TMP_CONFIG}
cat ${COMMON_CONFIG} >> ${TMP_CONFIG}
/tmp/scripts/wrap_config_file.sh ${TMP_CONFIG}

initdb ${PGDATA}

echo "unix_socket_directories = '/var/run/postgresql'" >> ${PGDATA}/postgresql.conf
echo "archive_mode = on" >> ${PGDATA}/postgresql.conf
echo "archive_command = '/usr/bin/timeout 600 /usr/bin/wal-g --config=${TMP_CONFIG} wal-push %p'" >> ${PGDATA}/postgresql.conf
echo "archive_timeout = 600" >> ${PGDATA}/postgresql.conf
echo "shared_preload_libraries = 'orioledb'" >> ${PGDATA}/postgresql.conf
echo "orioledb.main_buffers = 512MB" >> ${PGDATA}/postgresql.conf
echo "orioledb.undo_buffers = 256MB" >> ${PGDATA}/postgresql.conf
echo "max_wal_size = 8GB" >> ${PGDATA}/postgresql.conf

pg_ctl -D ${PGDATA} -w start

/tmp/scripts/wait_while_pg_not_ready.sh

wal-g --config=${TMP_CONFIG} delete everything FORCE --confirm

psql -d postgres -c "CREATE EXTENSION orioledb;"
psql -d postgres -c "CREATE TABLE o_test(a int) USING orioledb;"
psql -d postgres -c "INSERT INTO o_test VALUES (1), (5), (2);"
psql -d postgres -c "TABLE o_test;"
psql -d postgres -c "DROP TABLE o_test;"
psql -d postgres -c "DROP EXTENSION orioledb;"

pgbench -i -s 4 postgres
wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}

pgbench -i -s 8 postgres
pg_dumpall -f /tmp/dump1
pgbench -c 2 -T 100000000 -S &
sleep 1
wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}
pg_ctl -D ${PGDATA} stop
rm -rf $PGDATA

wal-g --config=${TMP_CONFIG} backup-fetch ${PGDATA} LATEST

touch ${PGDATA}/recovery.signal
echo "restore_command = 'echo \"WAL file restoration: %f, %p\" && /usr/bin/wal-g --config=${TMP_CONFIG} wal-fetch \"%f\" \"%p\"'" >> ${PGDATA}/postgresql.conf

pg_ctl -D ${PGDATA} -w start

/tmp/scripts/wait_while_pg_not_ready.sh

pg_dumpall -f /tmp/dump2

diff /tmp/dump1 /tmp/dump2

psql -f /tmp/scripts/amcheck.sql -v "ON_ERROR_STOP=1" postgres
wal-g --config=${TMP_CONFIG} delete everything FORCE --confirm

rm ${TMP_CONFIG}
/tmp/scripts/drop_pg.sh
