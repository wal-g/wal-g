#!/bin/bash
set -e -x
CONFIG_FILE="/tmp/configs/pax_storage_test_config.json"

COMMON_CONFIG="/tmp/configs/common_config.json"
TMP_CONFIG="/tmp/configs/tmp_config.json"
cat ${CONFIG_FILE} > ${TMP_CONFIG}
echo "," >> ${TMP_CONFIG}
cat ${COMMON_CONFIG} >> ${TMP_CONFIG}
/tmp/pg_scripts/wrap_config_file.sh ${TMP_CONFIG}
source /tmp/tests/test_functions/util.sh

bootstrap_gp_cluster
setup_wal_archiving

wal-g --config=${TMP_CONFIG} delete everything FORCE --confirm

# Build a database with a PAX table that exercises every kind of file:
#   - data file (always)
#   - .toast sidecar (large varlena)
#   - .visimap (DELETE row-level deletion)
psql -p 7000 -c "DROP DATABASE IF EXISTS pax_test"
psql -p 7000 -c "CREATE DATABASE pax_test"
psql -p 7000 -d pax_test -c "CREATE TABLE pax_t(a int, payload text) USING pax DISTRIBUTED BY (a);"
psql -p 7000 -d pax_test -c "INSERT INTO pax_t SELECT i, repeat('x', 32) FROM generate_series(1, 100) i;"
# Big varlena -> external TOAST file
psql -p 7000 -d pax_test -c "INSERT INTO pax_t VALUES (10001, repeat('y', 12 * 1024 * 1024));"
psql -p 7000 -d pax_test -c "SELECT COUNT(*) FROM pax_t;"

# 1st backup: every PAX file uploaded fresh
WALG_GP_PAXFILE_SIZE_THRESHOLD=0 wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}

wal-g st ls -r --config=${TMP_CONFIG}

# Verify that paxfiles/ prefix is populated
if ! (wal-g st ls -r --config=${TMP_CONFIG} | grep -q "paxfiles/"); then
  echo "Error: PAX shared storage is empty after first backup"
  exit 1
fi

PAX_OBJECTS_AFTER_1ST=$(wal-g st ls -r --config=${TMP_CONFIG} | grep -c "paxfiles/" || true)
echo "PAX objects after 1st backup: ${PAX_OBJECTS_AFTER_1ST}"

# 2nd backup: nothing changed in PAX -> all files should be skipped (dedup)
WALG_GP_PAXFILE_SIZE_THRESHOLD=0 wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}

PAX_OBJECTS_AFTER_2ND=$(wal-g st ls -r --config=${TMP_CONFIG} | grep -c "paxfiles/" || true)
if [ "${PAX_OBJECTS_AFTER_2ND}" -ne "${PAX_OBJECTS_AFTER_1ST}" ]; then
  echo "Error: PAX dedup did not work, expected ${PAX_OBJECTS_AFTER_1ST} objects, got ${PAX_OBJECTS_AFTER_2ND}"
  exit 1
fi

# 3rd backup: DELETE rows -> creates new visimap files; data/toast unchanged
psql -p 7000 -d pax_test -c "DELETE FROM pax_t WHERE a BETWEEN 1 AND 50;"
WALG_GP_PAXFILE_SIZE_THRESHOLD=0 wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}

PAX_OBJECTS_AFTER_3RD=$(wal-g st ls -r --config=${TMP_CONFIG} | grep -c "paxfiles/" || true)
if [ "${PAX_OBJECTS_AFTER_3RD}" -le "${PAX_OBJECTS_AFTER_2ND}" ]; then
  echo "Error: 3rd backup (after DELETE) did not produce new visimap files, expected > ${PAX_OBJECTS_AFTER_2ND}, got ${PAX_OBJECTS_AFTER_3RD}"
  exit 1
fi

# Pick the latest backup name and exercise delete-before to verify orphan cleanup wiring
backup_name=$(wal-g --config=${TMP_CONFIG} backup-list | tail -n 1 | cut -f 1 -d " ")
wal-g --config=${TMP_CONFIG} backup-list
wal-g --config=${TMP_CONFIG} delete before $backup_name --confirm
wal-g st ls -r --config=${TMP_CONFIG}

# Restore from latest and verify data integrity
stop_and_delete_cluster_dir

wal-g backup-fetch LATEST --in-place --config=${TMP_CONFIG}
prepare_cluster
start_cluster

# 50 rows survived the DELETE plus the 1 huge-toast row = 51 total
psql -p 7000 -d pax_test -c "SELECT COUNT(*) FROM pax_t;" | grep -E "\b51\b" && EXIT_STATUS=$? || EXIT_STATUS=$?
if [ "$EXIT_STATUS" -ne 0 ] ; then
    echo "Error: Failed to read the expected row count from pax_t after restore"
    exit 1
fi

# External-toast row must come back intact
psql -p 7000 -d pax_test -c "SELECT length(payload) FROM pax_t WHERE a = 10001;" | grep -E "\b$((12 * 1024 * 1024))\b" && EXIT_STATUS=$? || EXIT_STATUS=$?
if [ "$EXIT_STATUS" -ne 0 ] ; then
    echo "Error: External TOAST payload corrupted after restore"
    exit 1
fi

cleanup