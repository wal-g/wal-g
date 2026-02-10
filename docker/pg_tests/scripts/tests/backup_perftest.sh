#!/bin/sh
set -e -x

. /tmp/tests/test_functions/prepare_config.sh
prepare_config "/tmp/configs/backup_perftest_config.json"

BACKUP_PUSH_LOGS="/tmp/logs/pg_backup_perftest_push"
BACKUP_FETCH_LOGS="/tmp/logs/pg_backup_perftest_fetch"
echo "" > ${BACKUP_PUSH_LOGS}
echo "" > ${BACKUP_FETCH_LOGS}

initdb "${PGDATA}"

pg_ctl -D "${PGDATA}" -w start

wal-g --config=${TMP_CONFIG} delete everything FORCE --confirm

# push permanent and impermanent delta backups
du -hs "${PGDATA}" 2>/dev/null || true
sleep 1
pgbench -i -s 10 postgres
sleep 1
du -hs "${PGDATA}" 2>/dev/null || true

sleep 1
/usr/bin/time -v -a --output ${BACKUP_PUSH_LOGS} wal-g --config=${TMP_CONFIG} backup-push "${PGDATA}"

wal-g --config=${TMP_CONFIG} backup-list
/tmp/scripts/drop_pg.sh

first_backup_name=$(wal-g --config=${TMP_CONFIG} backup-list | sed '2q;d' | cut -f 1 -d " ")
/usr/bin/time -v -a --output ${BACKUP_FETCH_LOGS} wal-g --config=${TMP_CONFIG} backup-fetch "${PGDATA}" "$first_backup_name"
/tmp/scripts/drop_pg.sh
/tmp/scripts/parselogs.sh ${BACKUP_PUSH_LOGS}
/tmp/scripts/parselogs.sh ${BACKUP_FETCH_LOGS}
