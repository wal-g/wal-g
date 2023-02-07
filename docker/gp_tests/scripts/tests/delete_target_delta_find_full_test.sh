#!/bin/bash
set -e -x
CONFIG_FILE="/tmp/configs/delete_target_delta_find_full_test_config.json"

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

# remember the backup-list output
# later in the test we create new backups which should be deleted so lists should be identical
wal-g --config=${TMP_CONFIG} backup-list
lines_before_delete=`wal-g --config=${TMP_CONFIG} backup-list 2>&1 | wc -l`
wal-g --config=${TMP_CONFIG} backup-list 2>&1 | tail -n 2 > /tmp/list_before_delete

# create one full and two increments
for i in 1 2 3
do
    if [ $i -eq 1 ]; then
       modifier='--full'
    else
       modifier=''
    fi
    insert_data
    wal-g --config=${TMP_CONFIG} backup-push ${modifier}
done

# get the name of the second incremental backup
SECOND_INCREMENT=$(wal-g --config=${TMP_CONFIG} backup-list 2>&1 | awk 'NR==5 {print $1}')

# make two increments from the SECOND_INCREMENT
insert_data
wal-g --config=${TMP_CONFIG} backup-push

insert_data
wal-g --config=${TMP_CONFIG} backup-push

# delete the SECOND_INCREMENT with FIND_FULL, should leave only the first full backup w/ first increment
wal-g --config=${TMP_CONFIG} delete target FIND_FULL ${SECOND_INCREMENT} --confirm

wal-g --config=${TMP_CONFIG} backup-list

lines_after_delete=`wal-g --config=${TMP_CONFIG} backup-list 2>&1 | wc -l`
wal-g --config=${TMP_CONFIG} backup-list 2>&1 | tail -n 2 > /tmp/list_after_delete

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
