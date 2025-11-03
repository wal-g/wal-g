#!/bin/sh
set -e -x

CONFIG_FILE="/tmp/configs/backup_merge_test_config.json"
COMMON_CONFIG="/tmp/configs/common_config.json"
TMP_CONFIG="/tmp/configs/tmp_backup_merge_config.json"

cat ${CONFIG_FILE} > ${TMP_CONFIG}
echo "," >> ${TMP_CONFIG}
cat ${COMMON_CONFIG} >> ${TMP_CONFIG}
/tmp/scripts/wrap_config_file.sh ${TMP_CONFIG}

/usr/lib/postgresql/10/bin/initdb ${PGDATA}

echo "archive_mode = on" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_command = '/usr/bin/timeout 600 /usr/bin/wal-g --config=${TMP_CONFIG} wal-push %p'" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_timeout = 600" >> /var/lib/postgresql/10/main/postgresql.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start
/tmp/scripts/wait_while_pg_not_ready.sh

wal-g --config=${TMP_CONFIG} delete everything FORCE --confirm

pgbench -i -s 2 postgres

wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}
BASE_BACKUP_NAME=$(wal-g --config=${TMP_CONFIG} backup-list | tail -n 1 | cut -f 1 -d ' ')
echo "Base backup created: ${BASE_BACKUP_NAME}"

pgbench -c 1 -T 10 postgres

wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}
DELTA_BACKUP_NAME=$(wal-g --config=${TMP_CONFIG} backup-list | tail -n 1 | cut -f 1 -d ' ')
echo "Delta backup created: ${DELTA_BACKUP_NAME}"

if echo "${DELTA_BACKUP_NAME}" | grep -q "_D_"; then
    echo "Delta backup naming is correct: ${DELTA_BACKUP_NAME}"
else
    echo "Error: Delta backup should contain '_D_' in name"
    exit 1
fi

pgbench -c 1 -T 10 postgres

wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}
SECOND_DELTA_BACKUP_NAME=$(wal-g --config=${TMP_CONFIG} backup-list | tail -n 1 | cut -f 1 -d ' ')
echo "Second delta backup created: ${SECOND_DELTA_BACKUP_NAME}"

pg_dumpall -f /tmp/dump_before_merge

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -m smart -w stop

echo "Testing backup-merge command..."
wal-g --config=${TMP_CONFIG} backup-merge ${SECOND_DELTA_BACKUP_NAME}

MERGED_BACKUP_NAME=$(echo "${SECOND_DELTA_BACKUP_NAME}" | sed 's/_D_[^_]*//g')
echo "Expected merged backup name: ${MERGED_BACKUP_NAME}"

wal-g --config=${TMP_CONFIG} backup-list

if wal-g --config=${TMP_CONFIG} backup-list | grep -q "${MERGED_BACKUP_NAME}"; then
    echo "Merged backup found in backup list: ${MERGED_BACKUP_NAME}"
else
    echo "Error: Merged backup not found in backup list"
    exit 1
fi

# Verify cleanup: only merged backup remains
REMAINING_BACKUPS=$(wal-g --config=${TMP_CONFIG} backup-list | awk '{print $1}' | grep -E '^base_')
if [ "$(echo "${REMAINING_BACKUPS}" | wc -l)" -ne 1 ] || [ "${REMAINING_BACKUPS}" != "${MERGED_BACKUP_NAME}" ]; then
    echo "Error: cleanup failed. Expected only ${MERGED_BACKUP_NAME}, got:"
    echo "${REMAINING_BACKUPS}"
    wal-g --config=${TMP_CONFIG} backup-list
    exit 1
fi

/tmp/scripts/drop_pg.sh

echo "Testing restore from merged backup..."
wal-g --config=${TMP_CONFIG} backup-fetch ${PGDATA} ${MERGED_BACKUP_NAME}

echo "restore_command = 'echo \"WAL file restoration: %f, %p\"&& /usr/bin/wal-g --config=${TMP_CONFIG} wal-fetch \"%f\" \"%p\"'" > ${PGDATA}/recovery.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w -t 100500 start
/tmp/scripts/wait_while_pg_not_ready.sh

pg_dumpall -f /tmp/dump_after_merge

echo "Comparing dumps..."
if diff /tmp/dump_before_merge /tmp/dump_after_merge; then
    echo "SUCCESS: Dumps are identical - backup-merge works correctly"
else
    echo "ERROR: Dumps differ - backup-merge failed"
    exit 1
fi

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -m smart -w stop

echo "Backup merge test completed successfully"