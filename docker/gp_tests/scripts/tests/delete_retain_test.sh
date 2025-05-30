#!/bin/bash
set -e -x
CONFIG_FILE="/tmp/configs/delete_retain_test_config.json"
COMMON_CONFIG="/tmp/configs/common_config.json"
TMP_CONFIG="/tmp/configs/tmp_config.json"
cat ${CONFIG_FILE} > ${TMP_CONFIG}
echo "," >> ${TMP_CONFIG}
cat ${COMMON_CONFIG} >> ${TMP_CONFIG}
/tmp/pg_scripts/wrap_config_file.sh ${TMP_CONFIG}
source /tmp/tests/test_functions/util.sh

bootstrap_gp_cluster
setup_wal_archiving
enable_pitr_extension

wal-g --config=${TMP_CONFIG} delete everything FORCE --confirm

for i in 1 2 3 4
do
    insert_data
    wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}
done

wal-g --config=${TMP_CONFIG} backup-list
lines_before_delete=`wal-g --config=${TMP_CONFIG} backup-list | wc -l`
wal-g --config=${TMP_CONFIG} backup-list | tail -n 1 > /tmp/list_tail_before_delete

wal-g --config=${TMP_CONFIG} delete retain 1 --confirm

wal-g --config=${TMP_CONFIG} backup-list
lines_after_delete=`wal-g --config=${TMP_CONFIG} backup-list | wc -l`
wal-g --config=${TMP_CONFIG} backup-list | tail -n 1 > /tmp/list_tail_after_delete

if [ $(($lines_before_delete-3)) -ne $lines_after_delete ];
then
    echo $(($lines_before_delete-3)) > /tmp/before_delete
    echo $lines_after_delete > /tmp/after_delete
    echo "Delete retain test: wrong number of deleted lines"
    diff /tmp/before_delete /tmp/after_delete
fi

diff /tmp/list_tail_before_delete /tmp/list_tail_after_delete
cleanup
rm ${TMP_CONFIG}
