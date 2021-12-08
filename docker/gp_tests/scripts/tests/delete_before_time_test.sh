#!/bin/bash
set -e -x
CONFIG_FILE="/tmp/configs/delete_before_time_test_config.json"

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

for i in 1 2 3 4
do
    insert_data
    sleep 1
    wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}
done

# take time of the second last backup
backup_time=`wal-g --config=${TMP_CONFIG} backup-list | tail -n 2 | head -n 1 | cut -f 2 -d " "`

wal-g --config=${TMP_CONFIG} backup-list
lines_before_delete=`wal-g --config=${TMP_CONFIG} backup-list | wc -l`
wal-g --config=${TMP_CONFIG} backup-list | tail -n 2 > /tmp/list_tail_before_delete

wal-g --config=${TMP_CONFIG} delete before $backup_time --confirm

wal-g --config=${TMP_CONFIG} backup-list
lines_after_delete=`wal-g --config=${TMP_CONFIG} backup-list | wc -l`
wal-g --config=${TMP_CONFIG} backup-list | tail -n 2 > /tmp/list_tail_after_delete

if [ $(($lines_before_delete-2)) -ne $lines_after_delete ];
then
    echo $(($lines_before_delete-2)) > /tmp/before_delete
    echo $lines_after_delete > /tmp/after_delete
    echo "Delete before time test: wrong number of deleted lines"
    diff /tmp/before_delete /tmp/after_delete
fi

diff /tmp/list_tail_before_delete /tmp/list_tail_after_delete
cleanup
rm ${TMP_CONFIG}
