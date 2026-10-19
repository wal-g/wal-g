#!/bin/sh
set -e -x
. /tmp/tests/test_functions/prepare_config.sh
. /tmp/tests/test_functions/pg_compat.sh
CONFIG_FILE="/tmp/configs/copy_backup_test_config.json"

TMP_CONFIG="/tmp/configs/tmp_config.json"
prepare_config "${CONFIG_FILE}"

mkdir /tmp/copy_backup_test_storage
TO_CONFIG_FILE="/tmp/configs/copy_backup_to_test_config.json"
TO_TMP_CONFIG="/tmp/configs/to_tmp_config.json"
prepare_config "${TO_CONFIG_FILE}" "${TO_TMP_CONFIG}"

initdb ${PGDATA}

echo "archive_mode = on" >> ${PGDATA}/postgresql.conf
echo "archive_command = '/usr/bin/timeout 600 wal-g --config=${TMP_CONFIG} wal-push %p'" >> ${PGDATA}/postgresql.conf
echo "archive_timeout = 600" >> ${PGDATA}/postgresql.conf

pg_ctl -D ${PGDATA} -w start

wal-g --config=${TMP_CONFIG} st rm / --target=all || true

sleep 1
echo $WALG_DELTA_MAX_STEPS

# push a backup
pgbench -i -s 1 postgres &
sleep 1
wal-g --config=${TMP_CONFIG} backup-push "${PGDATA}"
wal-g --config=${TMP_CONFIG} backup-list

# copy backup with backup-name
backup_name=$(wal-g --config=${TMP_CONFIG} backup-list | tail -n 1 | cut -f 1 -d " ")
wal-g copy --backup-name=${backup_name} --from=${TMP_CONFIG} --to=${TO_TMP_CONFIG}
copied_backup_name=$(wal-g --config=${TO_TMP_CONFIG} backup-list | tail -n 1 | cut -f 1 -d " ")

if [ $backup_name != $copied_backup_name ];
then
    echo "Copying backup failed"
    exit 2
fi

# push some more backups
for i in 2 3
do
    pgbench -i -s 1 postgres &
    sleep 1
    wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}
done

# remember a backup...
backup_name=$(wal-g --config=${TMP_CONFIG} backup-list | tail -n 1 | cut -f 1 -d " ")

# ...and push backups again
for i in 2 3
do
    pgbench -i -s 1 postgres &
    sleep 1
    wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}
done

# copy that backup
wal-g copy --backup-name=${backup_name} --from=${TMP_CONFIG} --to=${TO_TMP_CONFIG}
copied_backup_name=$(wal-g --config=${TO_TMP_CONFIG} backup-list | tail -n 1 | cut -f 1 -d " ")

# check if backup copied
if [ "$backup_name" != "$copied_backup_name" ];
then
    echo "Copying backup failed"
    exit 2
fi

# Restore twice to create real histories for timelines 2 and 3, then copy a
# timeline-3 backup. Its history contains the ancestry needed without file 2.
wait
psql -v ON_ERROR_STOP=1 postgres -c "CREATE TABLE copy_history_marker AS SELECT 42 AS value;"
for expected_timeline in 00000002 00000003
do
    wal-g --config="${TMP_CONFIG}" backup-push "${PGDATA}" --full
    pg_ctl -D "${PGDATA}" -m fast -w stop
    rm -rf "${PGDATA}"
    wal-g --config="${TMP_CONFIG}" backup-fetch "${PGDATA}" LATEST
    {
        echo "restore_command = 'wal-g --config=${TMP_CONFIG} wal-fetch %f %p'"
        echo "recovery_target_timeline = 'latest'"
    } | write_recovery_settings
    pg_ctl -D "${PGDATA}" -w start
    /tmp/scripts/wait_while_pg_not_ready.sh
    actual_timeline=$(psql -At -v ON_ERROR_STOP=1 postgres -c "SELECT left(pg_walfile_name(pg_current_wal_lsn()), 8);")
    test "${actual_timeline}" = "${expected_timeline}"
done

wal-g --config="${TMP_CONFIG}" backup-push "${PGDATA}" --full
wal-g copy --backup-name=LATEST --from="${TMP_CONFIG}" --to="${TO_TMP_CONFIG}"
wal-g --config="${TMP_CONFIG}" st ls -r > /tmp/copy_source_objects
wal-g --config="${TO_TMP_CONFIG}" st ls -r > /tmp/copy_target_objects
grep -F '00000002.history' /tmp/copy_source_objects
grep -F '00000003.history' /tmp/copy_target_objects
if grep -F '00000002.history' /tmp/copy_target_objects; then
    echo "Copying timeline-3 backup included an unnecessary older history file"
    exit 1
fi

pg_ctl -D "${PGDATA}" -m fast -w stop
rm -rf "${PGDATA}"
wal-g --config="${TO_TMP_CONFIG}" backup-fetch "${PGDATA}" LATEST
# Keep the verification restore independent of the source archive.
echo "archive_command = 'true'" >> "${PGDATA}/postgresql.conf"
{
    echo "restore_command = 'wal-g --config=${TO_TMP_CONFIG} wal-fetch %f %p'"
    echo "recovery_target_timeline = 'latest'"
} | write_recovery_settings
pg_ctl -D "${PGDATA}" -w start
/tmp/scripts/wait_while_pg_not_ready.sh
test "$(psql -At -v ON_ERROR_STOP=1 postgres -c 'SELECT value FROM copy_history_marker;')" = 42

/tmp/scripts/drop_pg.sh
