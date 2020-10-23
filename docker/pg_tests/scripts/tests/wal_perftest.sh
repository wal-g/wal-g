#!/bin/sh
set -e -x

CONFIG_FILE="/tmp/configs/wal_perftest_config.json"

COMMON_CONFIG="/tmp/configs/common_config.json"
TMP_CONFIG="/tmp/configs/tmp_config.json"
cat ${CONFIG_FILE} > ${TMP_CONFIG}

echo "," >> ${TMP_CONFIG}
cat ${COMMON_CONFIG} >> ${TMP_CONFIG}
/tmp/scripts/wrap_config_file.sh ${TMP_CONFIG}

WAL_PUSH_LOGS="/tmp/logs/pg_wal_perftest_push"
WAL_FETCH_LOGS="/tmp/logs/pg_wal_perftest_fetch"
echo "" > ${WAL_PUSH_LOGS}
echo "" > ${WAL_FETCH_LOGS}

/usr/lib/postgresql/10/bin/initdb "${PGDATA}"
/usr/lib/postgresql/10/bin/pg_ctl -D "${PGDATA}" -w start

/tmp/scripts/wait_while_pg_not_ready.sh

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
