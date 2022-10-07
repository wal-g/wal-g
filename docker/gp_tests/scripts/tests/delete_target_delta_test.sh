#!/bin/bash
set -e -x
CONFIG_FILE="/tmp/configs/delete_target_delta_test_config.json"

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

# create full backup and incremental
for i in 1 2
do
    insert_data
    wal-g --config=${TMP_CONFIG} backup-push
done

# remember the name of the first increment
FIRST_INCREMENT=$(wal-g --config=${TMP_CONFIG} backup-list | awk 'END {print $1}')

# make the second full backup
wal-g --config=${TMP_CONFIG} backup-push --full

# make the increment from the second full backup
insert_data
wal-g --config=${TMP_CONFIG} backup-push

# remember the backup-list output with two full backups and two increments.
# later in the test we create new backups which should be deleted so lists should be identical
lines_before_delete=`wal-g --config=${TMP_CONFIG} backup-list | wc -l`
wal-g --config=${TMP_CONFIG} backup-list | tail -n 4 > /tmp/list_before_delete

# now make increment from the FIRST_INCREMENT, which will be deleted later
insert_data
wal-g --config=${TMP_CONFIG} backup-push --delta-from-name ${FIRST_INCREMENT}
INCREMENT_TO_DELETE=$(wal-g --config=${TMP_CONFIG} backup-list | awk 'END {print $1}')

# make the increment from the INCREMENT_TO_DELETE
insert_data
wal-g --config=${TMP_CONFIG} backup-push --delta-from-name ${INCREMENT_TO_DELETE}

# make one more increment from the INCREMENT_TO_DELETE
insert_data
wal-g --config=${TMP_CONFIG} backup-push --delta-from-name ${INCREMENT_TO_DELETE}

# delete the INCREMENT_TO_DELETE, should leave only the first full backup w/ first increment and the second full backup w/ first increment
wal-g --config=${TMP_CONFIG} delete target ${INCREMENT_TO_DELETE} --confirm

lines_after_delete=`wal-g --config=${TMP_CONFIG} backup-list | wc -l`
wal-g --config=${TMP_CONFIG} backup-list | tail -n 4 > /tmp/list_after_delete

if [ $(($lines_before_delete)) -ne $lines_after_delete ];
then
    echo $(($lines_before_delete)) > /tmp/before_delete
    echo $lines_after_delete > /tmp/after_delete
    echo "Wrong number of deleted lines"
    diff /tmp/before_delete /tmp/after_delete
fi


diff /tmp/list_before_delete /tmp/list_after_delete
cleanup
rm ${TMP_CONFIG}
