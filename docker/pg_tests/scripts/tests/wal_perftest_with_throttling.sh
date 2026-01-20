#!/bin/sh
set -e -x

rm -rf ${PGDATA}

. /tmp/tests/test_functions/prepare_config.sh
prepare_config "/tmp/configs/wal_perftest_throttling_config.json"

WAL_PUSH_LOGS="/tmp/logs/pg_wal_perftest_push"
WAL_FETCH_LOGS="/tmp/logs/pg_wal_perftest_fetch"
echo "" > ${WAL_PUSH_LOGS}
echo "" > ${WAL_FETCH_LOGS}

initdb "${PGDATA}"
pg_ctl -D "${PGDATA}" -w start

wal-g --config=${TMP_CONFIG} delete everything FORCE --confirm

pgbench -i -s 50 postgres
pgbench -c 100 -t 100 postgres

du -hs ${PGDATA}
sleep 1
WAL=$(ls -l ${PGDATA}/pg_wal | head -n2 | tail -n1 | egrep -o "[0-9A-F]{24}")

du -hs "${PGDATA}"

i=0
START=$(date +%s)
while [ "$i" -le 101 ];
do
    cp  "${PGDATA}"/pg_wal/"${WAL}" "${PGDATA}"/pg_wal/"${WAL}${i}"
    cp  "${PGDATA}"/pg_wal/"${WAL}" "${PGDATA}"/pg_wal/"${i}${WAL}"
    cp  "${PGDATA}"/pg_wal/"${WAL}" "${PGDATA}"/pg_wal/"${i}${WAL}${i}"
    wal-g --config=${TMP_CONFIG} wal-push "${PGDATA}"/pg_wal/"${WAL}${i}" &
    wal-g --config=${TMP_CONFIG} wal-push "${PGDATA}"/pg_wal/"${i}${WAL}" &
    wal-g --config=${TMP_CONFIG} wal-push "${PGDATA}"/pg_wal/"${i}${WAL}${i}" &
    i=$(( i + 1 ))
done
wait
END=$(date +%s)
DIFF=$(( $END - $START ))
echo "It took $DIFF seconds"
test $DIFF -le 100
/tmp/scripts/drop_pg.sh

i=0
while [ "$i" -le 101 ];
do
    wal-g --config=${TMP_CONFIG} wal-fetch "${WAL}${i}" "${PGDATA}${i}"
    wal-g --config=${TMP_CONFIG} wal-fetch "${i}${WAL}" "${PGDATA}${i}A"
    wal-g --config=${TMP_CONFIG} wal-fetch "${i}${WAL}${i}" "${PGDATA}${i}B"
    i=$(( i + 1 ))
done
sleep 1
/tmp/scripts/drop_pg.sh
/tmp/scripts/parselogs.sh ${WAL_PUSH_LOGS}
/tmp/scripts/parselogs.sh ${WAL_FETCH_LOGS}
