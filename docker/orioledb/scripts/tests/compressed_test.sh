#!/bin/sh

set -e -x
CONFIG_FILE="/tmp/configs/compressed_test_config.json"
COMMON_CONFIG="/tmp/configs/common_config.json"
TMP_CONFIG="/tmp/configs/tmp_config.json"
cat ${CONFIG_FILE} > ${TMP_CONFIG}
echo "," >> ${TMP_CONFIG}
cat ${COMMON_CONFIG} >> ${TMP_CONFIG}
/tmp/scripts/wrap_config_file.sh ${TMP_CONFIG}

initdb ${PGDATA}

echo "unix_socket_directories = '/var/run/postgresql'" >> ${PGDATA}/postgresql.conf
echo "archive_mode = on" >> ${PGDATA}/postgresql.conf
echo "archive_command = '/usr/bin/timeout 600 /usr/bin/wal-g --config=${TMP_CONFIG} wal-push %p'" >> ${PGDATA}/postgresql.conf
echo "archive_timeout = 600" >> ${PGDATA}/postgresql.conf
echo "shared_preload_libraries = 'orioledb'" >> ${PGDATA}/postgresql.conf
echo "orioledb.main_buffers = 512MB" >> ${PGDATA}/postgresql.conf
echo "orioledb.undo_buffers = 256MB" >> ${PGDATA}/postgresql.conf
echo "max_wal_size = 8GB" >> ${PGDATA}/postgresql.conf

pg_ctl -D ${PGDATA} -w start

/tmp/scripts/wait_while_pg_not_ready.sh

wal-g --config=${TMP_CONFIG} delete everything FORCE --confirm

psql -d postgres -f /tmp/scripts/compressed_prepare.sql
pgbench -Igvpf -i -s 4 postgres
wal-g --config=${TMP_CONFIG} backup-push ${PGDATA} --full

# Full backup's size is the baseline the delta backup below must be much
# smaller than.
FULL_BACKUP_SIZE=$(wal-g --config=${TMP_CONFIG} backup-list --json --detail | jq -r '.[-1].uncompressed_size')

pgbench -Id -i -s 4 postgres

psql -d postgres -f /tmp/scripts/compressed_prepare.sql
pgbench -Igvpf -i -s 8 postgres
pg_dumpall -f /tmp/dump1 --restrict-key=orioledbkey

# Fingerprint of the compressed table's content right before the delta
# backup-push below.
PGBENCH_ACCOUNTS_FINGERPRINT=$(psql -d postgres -tAc \
    "SELECT md5(string_agg(aid || ',' || abalance || ',' || filler, '' ORDER BY aid)) FROM pgbench_accounts")

pgbench -c 2 -T 100000000 -S &
sleep 1
wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}
DELTA_BACKUP_NAME=$(wal-g --config=${TMP_CONFIG} backup-list --json --detail | jq -r '.[-1].backup_name')
DELTA_BACKUP_SIZE=$(wal-g --config=${TMP_CONFIG} backup-list --json --detail | jq -r '.[-1].uncompressed_size')

case "${DELTA_BACKUP_NAME}" in
  *_D_*) ;;
  *) echo "Expected a delta backup, got '${DELTA_BACKUP_NAME}'" && exit 1 ;;
esac

# A true page-level/chkpNum-filtered diff of the compressed OrioleDB files
# should ship a small fraction of the full backup's size.
if [ "${DELTA_BACKUP_SIZE}" -ge $((FULL_BACKUP_SIZE / 2)) ]; then
    echo "Delta backup (${DELTA_BACKUP_SIZE}) is not meaningfully smaller than the full backup (${FULL_BACKUP_SIZE})"
    exit 1
fi
if [ "${DELTA_BACKUP_SIZE}" -le 0 ]; then
    echo "Delta backup reported a non-positive size (${DELTA_BACKUP_SIZE})"
    exit 1
fi

pg_ctl -D ${PGDATA} stop

# Restore the delta backup on its own into a scratch data directory with
# recovery_target=immediate, i.e. with the least possible WAL replayed on
# top of the fetched files.
DELTA_CHECK_PGDATA=/tmp/pgdata_delta_check
rm -rf ${DELTA_CHECK_PGDATA}
wal-g --config=${TMP_CONFIG} backup-fetch ${DELTA_CHECK_PGDATA} ${DELTA_BACKUP_NAME}
touch ${DELTA_CHECK_PGDATA}/recovery.signal
echo "archive_mode = off" >> ${DELTA_CHECK_PGDATA}/postgresql.conf
echo "restore_command = 'echo \"WAL file restoration: %f, %p\" && /usr/bin/wal-g --config=${TMP_CONFIG} wal-fetch \"%f\" \"%p\"'" >> ${DELTA_CHECK_PGDATA}/postgresql.conf
echo "recovery_target = 'immediate'" >> ${DELTA_CHECK_PGDATA}/postgresql.conf
echo "recovery_target_action = 'promote'" >> ${DELTA_CHECK_PGDATA}/postgresql.conf

# wait_while_pg_not_ready.sh reads $PGDATA, so swap it for the scratch check
# and restore it afterwards for the rest of the script.
ORIGINAL_PGDATA=${PGDATA}
export PGDATA=${DELTA_CHECK_PGDATA}

pg_ctl -D ${PGDATA} -w start
/tmp/scripts/wait_while_pg_not_ready.sh

RESTORED_FINGERPRINT=$(psql -d postgres -tAc \
    "SELECT md5(string_agg(aid || ',' || abalance || ',' || filler, '' ORDER BY aid)) FROM pgbench_accounts")

pg_ctl -D ${PGDATA} stop
rm -rf ${PGDATA}

export PGDATA=${ORIGINAL_PGDATA}

if [ "${PGBENCH_ACCOUNTS_FINGERPRINT}" != "${RESTORED_FINGERPRINT}" ]; then
    echo "Delta backup restore mismatch for pgbench_accounts"
    echo "expected: ${PGBENCH_ACCOUNTS_FINGERPRINT}"
    echo "got:      ${RESTORED_FINGERPRINT}"
    exit 1
fi

rm -rf $PGDATA

wal-g --config=${TMP_CONFIG} backup-fetch ${PGDATA} LATEST

touch ${PGDATA}/recovery.signal
echo "restore_command = 'echo \"WAL file restoration: %f, %p\" && /usr/bin/wal-g --config=${TMP_CONFIG} wal-fetch \"%f\" \"%p\"'" >> ${PGDATA}/postgresql.conf

pg_ctl -D ${PGDATA} -w start

/tmp/scripts/wait_while_pg_not_ready.sh

pg_dumpall -f /tmp/dump2 --restrict-key=orioledbkey

diff /tmp/dump1 /tmp/dump2

psql -f /tmp/scripts/orioledb_check.sql -v "ON_ERROR_STOP=1" postgres
psql -f /tmp/scripts/orioledb_compressed_check.sql -v "ON_ERROR_STOP=1" postgres
wal-g --config=${TMP_CONFIG} delete everything FORCE --confirm

rm ${TMP_CONFIG}
/tmp/scripts/drop_pg.sh
