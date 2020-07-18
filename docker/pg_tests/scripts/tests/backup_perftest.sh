#!/bin/sh
set -e -x

CONFIG_FILE="/tmp/configs/backup_perftest_config.json"

COMMON_CONFIG="/tmp/configs/common_config.json"
TMP_CONFIG="/tmp/configs/tmp_config.json"
cat ${CONFIG_FILE} > ${TMP_CONFIG}

echo "," >> ${TMP_CONFIG}
cat ${COMMON_CONFIG} >> ${TMP_CONFIG}
/tmp/scripts/wrap_config_file.sh ${TMP_CONFIG}

BACKUP_PUSH_LOGS="/tmp/logs/pg_backup_perftest_push"
BACKUP_FETCH_LOGS="/tmp/logs/pg_backup_perftest_fetch"
echo "" > ${BACKUP_PUSH_LOGS}
echo "" > ${BACKUP_FETCH_LOGS}

/usr/lib/postgresql/10/bin/initdb ${PGDATA}

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start

/tmp/scripts/wait_while_pg_not_ready.sh

wal-g --config=${TMP_CONFIG} delete everything FORCE --confirm

# push permanent and impermanent delta backups
du -hs ${PGDATA}
sleep 1
pgbench -i -s 10 postgres
sleep 1
du -hs ${PGDATA}

sleep 1
/usr/bin/time -v -a --output ${BACKUP_PUSH_LOGS} wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}

wal-g --config=${TMP_CONFIG} backup-list
/tmp/scripts/drop_pg.sh

first_backup_name=`wal-g --config=${TMP_CONFIG} backup-list | sed '2q;d' | cut -f 1 -d " "`
/usr/bin/time -v -a --output ${BACKUP_FETCH_LOGS} wal-g --config=${TMP_CONFIG} backup-fetch ${PGDATA} $first_backup_name
/tmp/scripts/drop_pg.sh
/tmp/scripts/parselogs.sh ${BACKUP_PUSH_LOGS}
/tmp/scripts/parselogs.sh ${BACKUP_FETCH_LOGS}

echo "Backup perftest success"
