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
psql -c "CREATE DATABASE third" postgres
psql -c "CREATE TABLE tbl1 (data integer); INSERT INTO tbl1 VALUES (1), (2);" first
psql -c "CREATE TABLE tbl2 (data integer); INSERT INTO tbl2 VALUES (3), (4);" second
psql -c "CREATE TABLE tbl3 (data integer); INSERT INTO tbl3 VALUES (5), (6);" third
sleep 1

wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}

psql -c "INSERT INTO tbl1 VALUES (7), (8);" first
psql -c "INSERT INTO tbl2 VALUES (9), (10);" second
psql -c "INSERT INTO tbl3 VALUES (11), (12);" third
psql -c "SELECT pg_switch_wal();" postgres
SECOND_OID=$(psql -t -c "SELECT oid FROM pg_database WHERE datname = 'second';" -d postgres -A;)
sleep 10


/tmp/scripts/drop_pg.sh
wal-g --config=${TMP_CONFIG} backup-fetch ${PGDATA} LATEST --restore-only=${SECOND_OID},first
echo "restore_command = 'echo \"WAL file restoration: %f, %p\"&& wal-g --config=${TMP_CONFIG} wal-fetch \"%f\" \"%p\"'" > ${PGDATA}/recovery.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start
/tmp/scripts/wait_while_pg_not_ready.sh

if [ "$(psql -t -c "select data from tbl1;" -d first -A)" = "$(printf '1\n2\n7\n8')" ]; then
  echo "First database partial backup backup success!!!!!!"
else
  echo "Partial backup doesn't work :("
  exit 1
fi

if [ "$(psql -t -c "select data from tbl2;" -d second -A)" = "$(printf '3\n4\n9\n10')" ]; then
  echo "Second database partial backup backup success!!!!!!"
else
  echo "Partial backup doesn't work :("
  exit 1
fi

if psql -t -c "select data from tbl3;" -d third -A 2>&1 | grep -q "is not a valid data directory"; then
  echo "Skipped [third] database raises error, as it should be!!!!!"
else
  echo "Skipped database responses unexpectedly"
  exit 1
fi