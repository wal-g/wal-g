#!/bin/sh
set -e -x

. /tmp/tests/test_functions/prepare_config.sh
prepare_config "/tmp/configs/prefetch_test_config.json"

WAL_PUSH_LOGS="/tmp/logs/pg_wal_perftest_push"
WAL_FETCH_LOGS="/tmp/logs/pg_wal_perftest_fetch"
echo "" > ${WAL_PUSH_LOGS}
echo "" > ${WAL_FETCH_LOGS}

wal-g --config=${TMP_CONFIG} delete everything FORCE --confirm

# create file with WalSegmentSize size
fallocate -l 1M /tmp/0000001100002BCB00000063 
# change first 4 bytes to pass wal file magic
echo -ne \\xFFFF\\xFFFF\\xFFFF\\xFFFF | dd conv=notrunc bs=4 count=1 of=/tmp/0000001100002BCB00000063

cp /tmp/0000001100002BCB00000063 /tmp/0000001100002BCB00000064
cp /tmp/0000001100002BCB00000063 /tmp/0000001100002BCB00000065

wal-g --config=${TMP_CONFIG} wal-push "/tmp/0000001100002BCB00000063"
wal-g --config=${TMP_CONFIG} wal-push "/tmp/0000001100002BCB00000064"
wal-g --config=${TMP_CONFIG} wal-push "/tmp/0000001100002BCB00000065"

rm -f /tmp/0000001100002BCB00000063
rm -f /tmp/0000001100002BCB00000064
rm -f /tmp/0000001100002BCB00000065


wal-g --config=${TMP_CONFIG} wal-fetch "0000001100002BCB00000063" "/tmp/0000001100002BCB00000063"

sleep 10

wal-g --config=${TMP_CONFIG} st rm wal_005/

wal-g --config=${TMP_CONFIG} wal-fetch "0000001100002BCB00000064" "/tmp/0000001100002BCB00000064"
wal-g --config=${TMP_CONFIG} wal-fetch "0000001100002BCB00000065" "/tmp/0000001100002BCB00000065"



/tmp/scripts/drop_pg.sh
/tmp/scripts/parselogs.sh ${WAL_PUSH_LOGS}
/tmp/scripts/parselogs.sh ${WAL_FETCH_LOGS}
