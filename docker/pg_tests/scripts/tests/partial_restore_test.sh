#!/bin/sh
set -e -x
CONFIG_FILE="/tmp/configs/partial_restore_test_config.json"
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
psql -c "CREATE TABLE tbl1 (data integer); INSERT INTO tbl1 SELECT * FROM generate_series(1, 10000)" first
psql -c "CREATE TABLE tbl2 (data integer); INSERT INTO tbl2 SELECT * FROM generate_series(1, 10000)" first
psql -c "CREATE TABLE tbl (data integer); INSERT INTO tbl SELECT * FROM generate_series(1, 10000)" second
psql -c "CREATE TABLE tbl (data integer); INSERT INTO tbl SELECT * FROM generate_series(1, 10000)" third
sleep 1

wal-g --config=${TMP_CONFIG} backup-push ${PGDATA} &

while ! test -f "${PGDATA}/.semaphore"; do
    sleep 1
    echo "Still waiting ${PGDATA}/.semaphore"
done

psql -c "INSERT INTO tbl1 SELECT * FROM generate_series(1, 10000)" first
psql -c "INSERT INTO tbl2 SELECT * FROM generate_series(1, 10000)" first
psql -c "INSERT INTO tbl SELECT * FROM generate_series(1, 10000)" second
psql -c "INSERT INTO tbl SELECT * FROM generate_series(1, 10000)" third

rm -f "${PGDATA}/.semaphore"

wait %1 ||  echo 'backup done no need to wait'

sleep 10

/tmp/scripts/drop_pg.sh
wal-g --config=${TMP_CONFIG} backup-fetch ${PGDATA} LATEST --restore-only=first/tbl1,second
echo "restore_command = 'echo \"WAL file restoration: %f, %p\"&& wal-g --config=${TMP_CONFIG} wal-fetch \"%f\" \"%p\"'" > ${PGDATA}/recovery.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start
/tmp/scripts/wait_while_pg_not_ready.sh

if [ "$(psql -t -c "SELECT COUNT(*) FROM tbl1;" -d first -A)" = 20000 ]; then
  echo "First database partial restore success!!!!!!"
else
  echo "Partial restore doesn't work :("
  exit 1
fi

if [ "$(psql -t -c "SELECT COUNT(*) FROM tbl2;" -d first -A)" -lt 20000 ];  then
  echo "Skipped table is not full, as it should be!!!!!"
else
  echo "Skipped table works unexpectedly"
  exit 1
fi

if [ "$(psql -t -c "SELECT COUNT(*) FROM tbl;" -d second -A)" = 20000 ]; then
  echo "Second database partial restore success!!!!!!"
else
  echo "Partial restore doesn't work :("
  exit 1
fi

if psql -t -c "SELECT COUNT(*) FROM tbl;" -d third -A 2>&1 | grep -q "is not a valid data directory"; then
  /tmp/scripts/drop_pg.sh
  echo "Skipped database raises error, as it should be!!!!!"
else
  echo "Skipped database responses unexpectedly"
  exit 1
fi
