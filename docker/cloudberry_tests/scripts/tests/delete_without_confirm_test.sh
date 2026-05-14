#!/bin/bash
set -e -x
CONFIG_FILE="/tmp/configs/delete_without_confirm_test_config.json"
COMMON_CONFIG="/tmp/configs/common_config.json"
TMP_CONFIG="/tmp/configs/tmp_config.json"
jq -s '.[0] * .[1]' ${COMMON_CONFIG} ${CONFIG_FILE} > ${TMP_CONFIG}
source /tmp/tests/test_functions/util.sh

bootstrap_gp_cluster
setup_wal_archiving

wal-g --config=${TMP_CONFIG} delete everything FORCE --confirm

for i in 1 2
do
    insert_data
    wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}
done

lines_before_delete=`wal-g --config=${TMP_CONFIG} backup-list | wc -l`
wal-g --config=${TMP_CONFIG} backup-list > /tmp/list_before_delete

wal-g --config=${TMP_CONFIG} delete retain 1

lines_after_delete=`wal-g --config=${TMP_CONFIG} backup-list | wc -l`
wal-g --config=${TMP_CONFIG} backup-list > /tmp/list_after_delete

if [ $lines_before_delete -ne $lines_after_delete ];
then
    echo $lines_before_delete > /tmp/before_delete
    echo $lines_after_delete > /tmp/after_delete
    echo "Backups were deleted without --confirm"
    diff /tmp/before_delete /tmp/after_delete
fi

diff /tmp/list_before_delete /tmp/list_after_delete
cleanup
rm ${TMP_CONFIG}
