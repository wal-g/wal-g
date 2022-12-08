#!/bin/bash
set -e -x
CONFIG_FILE="/tmp/configs/delete_garbage_test_config.json"
COMMON_CONFIG="/tmp/configs/common_config.json"
TMP_CONFIG="/tmp/configs/tmp_config.json"
cat ${CONFIG_FILE} > ${TMP_CONFIG}
echo "," >> ${TMP_CONFIG}
cat ${COMMON_CONFIG} >> ${TMP_CONFIG}
/tmp/pg_scripts/wrap_config_file.sh ${TMP_CONFIG}
source /tmp/tests/test_functions/util.sh

bootstrap_gp_cluster
sleep 3
setup_wal_archiving
enable_pitr_extension

wal-g --config=${TMP_CONFIG} delete everything FORCE --confirm

insert_data
sleep 1
wal-g --config=${TMP_CONFIG} backup-push --permanent

PERMANENT_BACKUP=$(wal-g --config=${TMP_CONFIG} backup-list | awk 'NR==2{print $1}')

# add some WALs
insert_a_lot_of_data
sleep 1

# make two non-permanent backups
for _ in 1 2
do
    insert_data
    sleep 1
    wal-g --config=${TMP_CONFIG} backup-push
done

FIRST_NON_PERMANENT_BACKUP=$(wal-g --config=${TMP_CONFIG} backup-list | awk 'NR==3{print $1}')

# backup the first non-permanent backup sentinel and remove it from the storage
# to emulate some partially deleted backup
# then try to delete the garbage (without the --confirm flag)
wal-g st cat "basebackups_005/${FIRST_NON_PERMANENT_BACKUP}_backup_stop_sentinel.json" > "/tmp/${FIRST_NON_PERMANENT_BACKUP}_backup_stop_sentinel.json" --config=${TMP_CONFIG}
wal-g st rm "basebackups_005/${FIRST_NON_PERMANENT_BACKUP}_backup_stop_sentinel.json" --config=${TMP_CONFIG}

# check that delete garbage works
wal-g delete garbage --config=${TMP_CONFIG} > /tmp/delete_garbage_default_output 2>&1
if ! grep -q "basebackups_005" /tmp/delete_garbage_default_output;
then
  echo "wal-g delete garbage did not delete any of the basebackups_005/* files!"
  cat /tmp/delete_garbage_default_output
  exit 1
fi
if ! grep -q "wal_005" /tmp/delete_garbage_default_output;
then
  echo "wal-g delete garbage did not delete any of the wal_005/* files!"
  cat /tmp/delete_garbage_default_output
  exit 1
fi

# restore the backup sentinel
wal-g st put "/tmp/${FIRST_NON_PERMANENT_BACKUP}_backup_stop_sentinel.json" "basebackups_005/${FIRST_NON_PERMANENT_BACKUP}_backup_stop_sentinel.json" --no-compress --no-encrypt --config=${TMP_CONFIG}

# delete the first non-permanent backup
wal-g --config=${TMP_CONFIG} delete target "${FIRST_NON_PERMANENT_BACKUP}" --confirm

# should delete WALs in ranges (0, PERMANENT_BACKUP) and (PERMANENT_BACKUP, second non-permanent backup)
wal-g --config=${TMP_CONFIG} delete garbage --confirm

FIRST_BACKUP=$(wal-g --config=${TMP_CONFIG} backup-list | awk 'NR==2{print $1}')

if [ "$PERMANENT_BACKUP" != "$FIRST_BACKUP" ];
then
    echo "oh no! delete garbage deleted the permanent backup!"
    exit 1
fi

# try to restore the permanent backup
stop_and_delete_cluster_dir

sleep 1
# should successfully restore the second delta chain
wal-g backup-fetch $PERMANENT_BACKUP --in-place --config=${TMP_CONFIG}
start_cluster

psql -p 6000 -d test -c "SELECT COUNT(*) FROM ao;" && EXIT_STATUS=$? || EXIT_STATUS=$?
if [ "$EXIT_STATUS" -ne 0 ] ; then
    echo "Error: Failed to read from ao table after restore"
    exit 1
fi

psql -p 6000 -d test -c "SELECT COUNT(*) FROM co;" && EXIT_STATUS=$? || EXIT_STATUS=$?
if [ "$EXIT_STATUS" -ne 0 ] ; then
    echo "Error: Failed to read from co table after restore"
    exit 1
fi

cleanup
rm ${TMP_CONFIG}
