#!/bin/sh
set -e -x

. /tmp/tests/test_functions/prepare_config.sh
prepare_config "/tmp/configs/wal_perftest_config.json"

WAL_PUSH_LOGS="/tmp/logs/pg_wal_perftest_push"
WAL_FETCH_LOGS="/tmp/logs/pg_wal_perftest_fetch"
echo "" > ${WAL_PUSH_LOGS}
echo "" > ${WAL_FETCH_LOGS}

initdb "${PGDATA}"
pg_ctl -D "${PGDATA}" -w start

wal-g --config=${TMP_CONFIG} delete everything FORCE --confirm

pgbench -i -s 50 postgres
du -hs ${PGDATA}
sleep 1
WAL=$(ls -l ${PGDATA}/pg_wal | head -n2 | tail -n1 | egrep -o "[0-9A-F]{24}")

du -hs "${PGDATA}"
/usr/bin/time -v -a --output ${WAL_PUSH_LOGS} wal-g --config=${TMP_CONFIG} wal-push "${PGDATA}"/pg_wal/"${WAL}"
sleep 1
/tmp/scripts/drop_pg.sh

/usr/bin/time -v -a --output ${WAL_FETCH_LOGS} wal-g --config=${TMP_CONFIG} wal-fetch "${WAL}" "${PGDATA}"
sleep 1
/tmp/scripts/drop_pg.sh
/tmp/scripts/parselogs.sh ${WAL_PUSH_LOGS}
/tmp/scripts/parselogs.sh ${WAL_FETCH_LOGS}
