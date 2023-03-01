#!/bin/sh
set -e -x
CONFIG_FILE="/tmp/configs/partial_backup_test_config.json"
COMMON_CONFIG="/tmp/configs/common_config.json"
TMP_CONFIG="/tmp/configs/tmp_config.json"
cat ${CONFIG_FILE} > ${TMP_CONFIG}
echo "," >> ${TMP_CONFIG}
cat ${COMMON_CONFIG} >> ${TMP_CONFIG}
/tmp/scripts/wrap_config_file.sh ${TMP_CONFIG}

wal-g --config=${TMP_CONFIG} delete everything FORCE --confirm

/usr/lib/postgresql/10/bin/initdb ${PGDATA}

echo "archive_mode = on" >> ${PGDATA}/postgresql.conf
echo "archive_command = 'wal-g --config=${TMP_CONFIG} wal-push %p && echo \"WAL pushing: %p\"'" >> ${PGDATA}/postgresql.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start
/tmp/scripts/wait_while_pg_not_ready.sh

psql -c "CREATE DATABASE first" postgres
psql -c "CREATE DATABASE second" postgres
psql -c "CREATE TABLE tbl1 (data integer); INSERT INTO tbl1 VALUES (1), (2);" first
psql -c "CREATE TABLE tbl2 (data integer); INSERT INTO tbl2 VALUES (3), (4);" second
sleep 1

wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}

psql -c "INSERT INTO tbl1 VALUES (5), (6);" first
psql -c "INSERT INTO tbl2 VALUES (7), (8);" second
FIRST_OID=$(psql -t -c "SELECT oid FROM pg_database WHERE datname = 'first';" -d postgres -A;)
T0_OID=$(psql -t -c "SELECT oid FROM pg_database WHERE datname = 'template0';" -d postgres -A;)
T1_OID=$(psql -t -c "SELECT oid FROM pg_database WHERE datname = 'template1';" -d postgres -A;)
PG_OID=$(psql -t -c "SELECT oid FROM pg_database WHERE datname = 'postgres';" -d postgres -A;)
psql -c "SELECT pg_switch_wal();" postgres
sleep 10

/tmp/scripts/drop_pg.sh
wal-g --config=${TMP_CONFIG} backup-fetch ${PGDATA} LATEST --restore-only=${T1_OID},${T0_OID},${PG_OID},${FIRST_OID}
echo "restore_command = 'echo \"WAL file restoration: %f, %p\"&& wal-g --config=${TMP_CONFIG} wal-fetch \"%f\" \"%p\"'" > ${PGDATA}/recovery.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start
/tmp/scripts/wait_while_pg_not_ready.sh

if psql -t -c "select data from tbl1;" -d first -A | grep -q "$(printf '1\n2\n5\n6')"; then
  echo "Partial backup success!!!!!!"
else
  echo "Partial backup doesn't work :("
  exit 1
fi
