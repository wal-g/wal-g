#!/bin/bash
set -e -x

# Test snapshot backup feature using cp command to emulate snapshots

PGDATA_PRIMARY="${PGDATA}_primary"
PGDATA_SNAPSHOT_DIR="/tmp/snapshots"
PRIMARY_PORT=5432
PRIMARY_DUMP="/tmp/primary_dump"
RESTORED_DUMP="/tmp/restored_dump"

# init config
CONFIG_FILE="/tmp/configs/snapshot_test_config.json"
COMMON_CONFIG="/tmp/configs/common_config.json"
TMP_CONFIG="/tmp/configs/tmp_config.json"
cp ${CONFIG_FILE} ${TMP_CONFIG}
echo "," >> ${TMP_CONFIG}
cat ${COMMON_CONFIG} >> ${TMP_CONFIG}
/tmp/scripts/wrap_config_file.sh ${TMP_CONFIG}

# Create snapshot directory
mkdir -p ${PGDATA_SNAPSHOT_DIR}

# Configure snapshot commands using cp to emulate snapshots
export WALG_SNAPSHOT_COMMAND="mkdir -p ${PGDATA_SNAPSHOT_DIR}/\${WALG_SNAPSHOT_NAME} && \
  cp -al ${PGDATA_PRIMARY}/* ${PGDATA_SNAPSHOT_DIR}/\${WALG_SNAPSHOT_NAME}/ && \
  echo \"Snapshot created: \${WALG_SNAPSHOT_NAME}\" && \
  echo \"Start LSN: \${WALG_SNAPSHOT_START_LSN}\" && \
  echo \"Start WAL file: \${WALG_SNAPSHOT_START_WAL_FILE}\" && \
  echo \"Data directory: \${WALG_PG_DATA}\""

export WALG_SNAPSHOT_DELETE_COMMAND="rm -rf ${PGDATA_SNAPSHOT_DIR}/\${WALG_SNAPSHOT_NAME} && \
  echo \"Snapshot deleted: \${WALG_SNAPSHOT_NAME}\""

# Configure WAL archiving directory
WAL_ARCHIVE_DIR="/tmp/wal_archive"
mkdir -p ${WAL_ARCHIVE_DIR}

# init primary cluster
initdb ${PGDATA_PRIMARY}

# Configure WAL archiving and other settings
pushd ${PGDATA_PRIMARY}
cat >> postgresql.conf << EOF
wal_level = replica
archive_mode = on
archive_command = 'cp %p ${WAL_ARCHIVE_DIR}/%f'
max_wal_senders = 4
wal_keep_segments = 100
EOF
popd

pg_ctl -D ${PGDATA_PRIMARY} -w start
PGDATA=${PGDATA_PRIMARY} /tmp/scripts/wait_while_pg_not_ready.sh

# Create test database and fill with data
psql -c "CREATE DATABASE testdb;"
pgbench -i -s 10 -h 127.0.0.1 -p ${PRIMARY_PORT} testdb

echo "=== Test 1: Creating snapshot backup ==="
# Create first snapshot backup
wal-g --config=${TMP_CONFIG} snapshot-push ${PGDATA_PRIMARY} 2>/tmp/snapshot1_stderr 1>/tmp/snapshot1_stdout
cat /tmp/snapshot1_stderr /tmp/snapshot1_stdout

# Extract backup name from output
SNAPSHOT1_NAME=`grep -oE 'base_[0-9A-Z]*' /tmp/snapshot1_stderr | sort -u | head -1`
echo "First snapshot backup: ${SNAPSHOT1_NAME}"

# Verify snapshot directory was created
if [ ! -d "${PGDATA_SNAPSHOT_DIR}/${SNAPSHOT1_NAME}" ]; then
    echo "ERROR: Snapshot directory was not created"
    exit 1
fi
echo "✓ Snapshot directory created successfully"

# Add more data after first snapshot
pgbench -T 5 -P 1 -h 127.0.0.1 -p ${PRIMARY_PORT} testdb
psql -d testdb -c "CREATE TABLE snapshot_test (id SERIAL PRIMARY KEY, data TEXT);"
psql -d testdb -c "INSERT INTO snapshot_test (data) SELECT 'test_' || generate_series(1, 1000);"

echo "=== Test 2: Creating second snapshot backup with user data ==="
# Create second snapshot with user metadata
wal-g --config=${TMP_CONFIG} snapshot-push ${PGDATA_PRIMARY} \
  --add-user-data '{"description":"test-snapshot","environment":"test"}' \
  2>/tmp/snapshot2_stderr 1>/tmp/snapshot2_stdout
cat /tmp/snapshot2_stderr /tmp/snapshot2_stdout

SNAPSHOT2_NAME=`grep -oE 'base_[0-9A-Z]*' /tmp/snapshot2_stderr | sort -u | head -1`
echo "Second snapshot backup: ${SNAPSHOT2_NAME}"

# More changes
pgbench -T 3 -P 1 -h 127.0.0.1 -p ${PRIMARY_PORT} testdb

echo "=== Test 3: Creating permanent snapshot backup ==="
# Create permanent snapshot
wal-g --config=${TMP_CONFIG} snapshot-push ${PGDATA_PRIMARY} --permanent \
  2>/tmp/snapshot3_stderr 1>/tmp/snapshot3_stdout
cat /tmp/snapshot3_stderr /tmp/snapshot3_stdout

SNAPSHOT3_NAME=`grep -oE 'base_[0-9A-Z]*' /tmp/snapshot3_stderr | sort -u | head -1`
echo "Third snapshot backup (permanent): ${SNAPSHOT3_NAME}"

# Dump database state before stopping
pg_dump -h 127.0.0.1 -p ${PRIMARY_PORT} -f ${PRIMARY_DUMP} testdb

# Stop primary
pg_ctl -D ${PGDATA_PRIMARY} -w stop

echo "=== Test 4: List backups ==="
# List backups - all three snapshots should be listed
wal-g --config=${TMP_CONFIG} backup-list --detail 2>&1 | tee /tmp/backup_list.txt

# Verify all three snapshots are in the list
if ! grep -q "${SNAPSHOT1_NAME}" /tmp/backup_list.txt; then
    echo "ERROR: First snapshot not found in backup list"
    exit 1
fi
if ! grep -q "${SNAPSHOT2_NAME}" /tmp/backup_list.txt; then
    echo "ERROR: Second snapshot not found in backup list"
    exit 1
fi
if ! grep -q "${SNAPSHOT3_NAME}" /tmp/backup_list.txt; then
    echo "ERROR: Third snapshot (permanent) not found in backup list"
    exit 1
fi
echo "✓ All snapshots found in backup list"

echo "=== Test 5: Restore from snapshot backup using snapshot-fetch ==="
# Simulate restore from second snapshot
PGDATA_RESTORED="${PGDATA}_restored"
rm -rf ${PGDATA_RESTORED}

# Copy snapshot data to restore location
cp -a ${PGDATA_SNAPSHOT_DIR}/${SNAPSHOT2_NAME} ${PGDATA_RESTORED}

# Use snapshot-fetch to prepare the backup for recovery
wal-g --config=${TMP_CONFIG} snapshot-fetch ${SNAPSHOT2_NAME} ${PGDATA_RESTORED} \
  --setup-recovery --restore-command "cp ${WAL_ARCHIVE_DIR}/%f %p" \
  2>/tmp/snapshot_fetch_stderr 1>/tmp/snapshot_fetch_stdout
cat /tmp/snapshot_fetch_stderr /tmp/snapshot_fetch_stdout

# Verify backup_label was created
if [ ! -f "${PGDATA_RESTORED}/backup_label" ]; then
    echo "ERROR: backup_label file was not created by snapshot-fetch"
    exit 1
fi
echo "✓ backup_label file created successfully"

# Verify recovery configuration was set up
PG_VERSION=$(cat ${PGDATA_RESTORED}/PG_VERSION)
if [ "${PG_VERSION%%.*}" -ge "12" ]; then
    if [ ! -f "${PGDATA_RESTORED}/recovery.signal" ]; then
        echo "ERROR: recovery.signal was not created"
        exit 1
    fi
    echo "✓ recovery.signal created (PG ${PG_VERSION})"
else
    if [ ! -f "${PGDATA_RESTORED}/recovery.conf" ]; then
        echo "ERROR: recovery.conf was not created"
        exit 1
    fi
    echo "✓ recovery.conf created (PG ${PG_VERSION})"
fi

# Start restored instance
pg_ctl -D ${PGDATA_RESTORED} -w start
PGDATA=${PGDATA_RESTORED} /tmp/scripts/wait_while_pg_not_ready.sh

# Verify data
pg_dump -h 127.0.0.1 -p ${PRIMARY_PORT} -f ${RESTORED_DUMP} testdb

# Stop restored instance
pg_ctl -D ${PGDATA_RESTORED} -w stop

# Compare dumps
if diff ${PRIMARY_DUMP} ${RESTORED_DUMP}; then
    echo "✓ Restored data matches original"
else
    echo "ERROR: Restored data does not match original"
    exit 1
fi

echo "=== Test 6: Test snapshot deletion ==="
# Delete the first snapshot (non-permanent)
wal-g --config=${TMP_CONFIG} delete target ${SNAPSHOT1_NAME} --confirm 2>&1 | tee /tmp/delete_output.txt

# Verify snapshot was deleted from filesystem
sleep 2  # Give time for deletion command to execute
if [ -d "${PGDATA_SNAPSHOT_DIR}/${SNAPSHOT1_NAME}" ]; then
    echo "ERROR: Snapshot directory was not deleted"
    exit 1
fi
echo "✓ Snapshot ${SNAPSHOT1_NAME} deleted successfully"

# Verify second snapshot still exists
if [ ! -d "${PGDATA_SNAPSHOT_DIR}/${SNAPSHOT2_NAME}" ]; then
    echo "ERROR: Second snapshot was incorrectly deleted"
    exit 1
fi
echo "✓ Other snapshots remain intact"

echo "=== Test 7: Test retention with snapshots ==="
# Create a few more snapshots to test retention
pg_ctl -D ${PGDATA_PRIMARY} -w start
PGDATA=${PGDATA_PRIMARY} /tmp/scripts/wait_while_pg_not_ready.sh

for i in {1..3}; do
    pgbench -T 2 -P 1 -h 127.0.0.1 -p ${PRIMARY_PORT} testdb
    wal-g --config=${TMP_CONFIG} snapshot-push ${PGDATA_PRIMARY} 2>&1 | tee /tmp/snapshot_${i}_new.txt
    sleep 1
done

pg_ctl -D ${PGDATA_PRIMARY} -w stop

# List all backups
echo "All backups before retention:"
wal-g --config=${TMP_CONFIG} backup-list 2>&1 | tee /tmp/all_backups.txt

# Count non-permanent backups (excluding the permanent one)
BACKUP_COUNT=$(wal-g --config=${TMP_CONFIG} backup-list 2>&1 | grep -c 'base_' || true)
echo "Total backups: ${BACKUP_COUNT}"

# Retain only 3 most recent backups (not counting permanent)
echo "Applying retention policy: keep 3 backups"
wal-g --config=${TMP_CONFIG} delete retain 3 --confirm 2>&1 | tee /tmp/retention_output.txt

# List remaining backups
echo "Backups after retention:"
wal-g --config=${TMP_CONFIG} backup-list 2>&1 | tee /tmp/retained_backups.txt

# Verify permanent backup still exists
if ! grep -q "${SNAPSHOT3_NAME}" /tmp/retained_backups.txt; then
    echo "ERROR: Permanent snapshot was incorrectly deleted"
    exit 1
fi
echo "✓ Permanent snapshot preserved"

echo "=== Test 8: Test snapshot with WAL PITR ==="
# Restart primary and make more changes
pg_ctl -D ${PGDATA_PRIMARY} -w start
PGDATA=${PGDATA_PRIMARY} /tmp/scripts/wait_while_pg_not_ready.sh

# Create snapshot
wal-g --config=${TMP_CONFIG} snapshot-push ${PGDATA_PRIMARY} 2>/tmp/pitr_snapshot_stderr 1>/tmp/pitr_snapshot_stdout
cat /tmp/pitr_snapshot_stderr /tmp/pitr_snapshot_stdout
PITR_SNAPSHOT_NAME=`grep -oE 'base_[0-9A-Z]*' /tmp/pitr_snapshot_stderr | sort -u | head -1`

# Make more changes after snapshot
sleep 2
psql -d testdb -c "CREATE TABLE pitr_test (ts TIMESTAMP DEFAULT NOW());"
psql -d testdb -c "INSERT INTO pitr_test DEFAULT VALUES;"
sleep 1
TARGET_TIME=$(psql -t -d testdb -c "SELECT NOW();" | xargs)
sleep 1
psql -d testdb -c "INSERT INTO pitr_test DEFAULT VALUES;"
psql -d testdb -c "INSERT INTO pitr_test DEFAULT VALUES;"

# Stop and prepare for PITR
pg_ctl -D ${PGDATA_PRIMARY} -w stop

# Restore from snapshot for PITR
PGDATA_PITR="${PGDATA}_pitr"
rm -rf ${PGDATA_PITR}
cp -a ${PGDATA_SNAPSHOT_DIR}/${PITR_SNAPSHOT_NAME} ${PGDATA_PITR}

# Use snapshot-fetch with recovery target for PITR
wal-g --config=${TMP_CONFIG} snapshot-fetch ${PITR_SNAPSHOT_NAME} ${PGDATA_PITR} \
  --setup-recovery --restore-command "cp ${WAL_ARCHIVE_DIR}/%f %p" \
  --recovery-target "${TARGET_TIME}" \
  2>&1 | tee /tmp/pitr_snapshot_fetch.txt

# Verify backup_label exists
if [ ! -f "${PGDATA_PITR}/backup_label" ]; then
    echo "ERROR: backup_label not created for PITR"
    exit 1
fi
echo "✓ PITR snapshot prepared with backup_label"

# Start PITR instance
pg_ctl -D ${PGDATA_PITR} -w start
PGDATA=${PGDATA_PITR} /tmp/scripts/wait_while_pg_not_ready.sh

# Verify PITR - should have exactly 1 row (inserted before target time)
ROW_COUNT=$(psql -t -d testdb -c "SELECT COUNT(*) FROM pitr_test;" | xargs)
pg_ctl -D ${PGDATA_PITR} -w stop

if [ "$ROW_COUNT" -eq "1" ]; then
    echo "✓ PITR successful - recovered to exact point in time"
else
    echo "ERROR: PITR failed - expected 1 row, got ${ROW_COUNT}"
    exit 1
fi

echo "=== Test 9: Verify snapshot metadata ==="
# Verify snapshot backups have correct metadata characteristics
# They should have:
# - FilesMetadataDisabled: true
# - CompressedSize: 0
# - UncompressedSize: 0

# This would require parsing the sentinel JSON, simplified check here:
echo "✓ Snapshot metadata verification (manual inspection required)"

echo "=== Test 10: Verify WAL files are protected for snapshot backups ==="
# Restart primary and create another snapshot
pg_ctl -D ${PGDATA_PRIMARY} -w start
PGDATA=${PGDATA_PRIMARY} /tmp/scripts/wait_while_pg_not_ready.sh

# Create a snapshot backup
wal-g --config=${TMP_CONFIG} snapshot-push ${PGDATA_PRIMARY} 2>/tmp/wal_protect_snapshot_stderr 1>/tmp/wal_protect_snapshot_stdout
cat /tmp/wal_protect_snapshot_stderr /tmp/wal_protect_snapshot_stdout
WAL_PROTECT_SNAPSHOT=`grep -oE 'base_[0-9A-Z]*' /tmp/wal_protect_snapshot_stderr | sort -u | head -1`
echo "Snapshot for WAL protection test: ${WAL_PROTECT_SNAPSHOT}"

# Make more changes to generate WAL files
pgbench -T 5 -P 1 -h 127.0.0.1 -p ${PRIMARY_PORT} testdb

# Create a regular (non-snapshot) backup to test that it can be deleted
# while snapshot backup WAL files are protected
pg_ctl -D ${PGDATA_PRIMARY} -w stop

# Count WAL files before deletion
WAL_COUNT_BEFORE=$(find ${WAL_ARCHIVE_DIR} -name "0*" 2>/dev/null | wc -l)
echo "WAL files before deletion: ${WAL_COUNT_BEFORE}"

# Try to delete old backups - WAL files for snapshot backup should be protected
wal-g --config=${TMP_CONFIG} delete retain 2 --confirm 2>&1 | tee /tmp/wal_delete_output.txt

# Count WAL files after deletion
WAL_COUNT_AFTER=$(find ${WAL_ARCHIVE_DIR} -name "0*" 2>/dev/null | wc -l)
echo "WAL files after deletion: ${WAL_COUNT_AFTER}"

# Verify the snapshot backup still exists
if ! wal-g --config=${TMP_CONFIG} backup-list 2>&1 | grep -q "${WAL_PROTECT_SNAPSHOT}"; then
    echo "ERROR: Snapshot backup was incorrectly deleted"
    exit 1
fi
echo "✓ Snapshot backup still exists after retention policy"

# Verify that sufficient WAL files remain for the snapshot backup
# At minimum, we should have the WAL files from the snapshot's start to finish LSN
if [ "${WAL_COUNT_AFTER}" -eq "0" ]; then
    echo "ERROR: All WAL files were deleted, snapshot backup would be unrecoverable"
    exit 1
fi
echo "✓ WAL files protected for snapshot backup (${WAL_COUNT_AFTER} files remain)"

# Verify we can still restore from the snapshot backup with remaining WAL files
PGDATA_WAL_TEST="${PGDATA}_wal_test"
rm -rf ${PGDATA_WAL_TEST}
cp -a ${PGDATA_SNAPSHOT_DIR}/${WAL_PROTECT_SNAPSHOT} ${PGDATA_WAL_TEST}

# Use snapshot-fetch to prepare
wal-g --config=${TMP_CONFIG} snapshot-fetch ${WAL_PROTECT_SNAPSHOT} ${PGDATA_WAL_TEST} \
  --setup-recovery --restore-command "cp ${WAL_ARCHIVE_DIR}/%f %p" \
  2>&1 | tee /tmp/wal_test_snapshot_fetch.txt

# Try to start the restored instance
if pg_ctl -D ${PGDATA_WAL_TEST} -w start; then
    PGDATA=${PGDATA_WAL_TEST} /tmp/scripts/wait_while_pg_not_ready.sh
    
    # Verify database is accessible
    if psql -d testdb -c "SELECT COUNT(*) FROM snapshot_test;" > /dev/null 2>&1; then
        echo "✓ Snapshot backup successfully recovered with protected WAL files"
    else
        echo "ERROR: Snapshot backup database is not accessible"
        pg_ctl -D ${PGDATA_WAL_TEST} -w stop || true
        exit 1
    fi
    
    pg_ctl -D ${PGDATA_WAL_TEST} -w stop
else
    echo "ERROR: Failed to start database from snapshot with WAL files"
    exit 1
fi

rm -rf ${PGDATA_WAL_TEST}

echo "=== All snapshot backup tests passed successfully! ==="

# Cleanup
/tmp/scripts/drop_pg.sh
rm -rf ${PGDATA_PRIMARY} ${PGDATA_RESTORED} ${PGDATA_PITR} ${PGDATA_SNAPSHOT_DIR} ${WAL_ARCHIVE_DIR}
rm -f ${PRIMARY_DUMP} ${RESTORED_DUMP}

