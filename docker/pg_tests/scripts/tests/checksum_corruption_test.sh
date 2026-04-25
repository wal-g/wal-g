#!/bin/sh
set -e -x

. /tmp/tests/test_functions/prepare_config.sh
prepare_config "/tmp/configs/checksum_corruption_test_config.json"

# Init cluster with data checksums enabled
initdb --data-checksums ${PGDATA}

echo "full_page_writes = on" >> ${PGDATA}/postgresql.conf
echo "archive_mode = on" >> ${PGDATA}/postgresql.conf
echo "archive_command = '/usr/bin/timeout 600 wal-g --config=${TMP_CONFIG} wal-push %p'" >> ${PGDATA}/postgresql.conf

pg_ctl -D ${PGDATA} -w start

wal-g --config=${TMP_CONFIG} st rm / --target=all || true

pgbench -i -s 1 postgres
psql -c "CHECKPOINT" postgres

# Target pgbench_accounts — the large table we just created (scale=1 → ~13MB).
# pg_relation_filepath returns the path relative to PGDATA, e.g. "base/16384/16385".
DATA_FILE="${PGDATA}/$(psql -t -A -c "SELECT pg_relation_filepath('pgbench_accounts')" postgres)"

# Keep a clean copy so test 2 starts from a valid state
cp "${DATA_FILE}" "/tmp/clean_data_file_backup"

pg_ctl -D ${PGDATA} -w stop

# ── Test 1: replace bytes (conv=notrunc) → file size unchanged, checksum breaks ──
# Equivalent to: dd if=/dev/urandom of=<file> bs=1 count=4 seek=6000 conv=notrunc
dd if=/dev/urandom of="${DATA_FILE}" bs=1 count=4 seek=6000 conv=notrunc

pg_ctl -D ${PGDATA} -w start

BACKUP_LOG_1="/tmp/checksum_test1.log"
wal-g --config=${TMP_CONFIG} backup-push ${PGDATA} --verify > "${BACKUP_LOG_1}" 2>&1 || true

if ! grep -q "Corruption found" "${BACKUP_LOG_1}"; then
    echo "TEST 1 FAILED: Expected 'Corruption found' warning for replaced bytes"
    cat "${BACKUP_LOG_1}"
    exit 1
fi
echo "TEST 1 PASSED: byte replacement detected as checksum corruption"

pg_ctl -D ${PGDATA} -w stop

# Restore the clean file before test 2
cp "/tmp/clean_data_file_backup" "${DATA_FILE}"

# ── Test 2: append bytes → file size grows by 4 (not a multiple of page size) ──
# Equivalent to: dd if=/dev/urandom bs=1 count=4 >> <file>
dd if=/dev/urandom bs=1 count=4 >> "${DATA_FILE}"

pg_ctl -D ${PGDATA} -w start

BACKUP_LOG_2="/tmp/checksum_test2.log"
wal-g --config=${TMP_CONFIG} backup-push ${PGDATA} --verify > "${BACKUP_LOG_2}" 2>&1 || true

if ! grep -q "invalid file size" "${BACKUP_LOG_2}"; then
    echo "TEST 2 FAILED: Expected 'invalid file size' warning for appended bytes"
    cat "${BACKUP_LOG_2}"
    exit 1
fi
if grep -q "Corruption found" "${BACKUP_LOG_2}"; then
    echo "TEST 2 FAILED: Got 'Corruption found' but size mismatch must take a different code path"
    cat "${BACKUP_LOG_2}"
    exit 1
fi
echo "TEST 2 PASSED: byte insertion hits size-guard, not checksum path"

/tmp/scripts/drop_pg.sh
rm ${TMP_CONFIG}
echo "checksum_corruption_test completed"
