#!/bin/bash
set -e -x
CONFIG_FILE="/tmp/configs/shared_size_test_config.json"

COMMON_CONFIG="/tmp/configs/common_config.json"
TMP_CONFIG="/tmp/configs/tmp_config.json"
cat ${CONFIG_FILE} > ${TMP_CONFIG}
echo "," >> ${TMP_CONFIG}
cat ${COMMON_CONFIG} >> ${TMP_CONFIG}
/tmp/pg_scripts/wrap_config_file.sh ${TMP_CONFIG}
source /tmp/tests/test_functions/util.sh

# The cluster backups, oldest first. Taken from the sentinels rather than from the journals: this
# test pushes backups without --count-journals, so there are no journals to derive them from.
get_backup_name() {
    wal-g --config=${TMP_CONFIG} st ls basebackups_005/ 2>&1 \
        | awk '{ print $NF }' | grep _backup_stop_sentinel.json \
        | sed 's/_backup_stop_sentinel.json//' | sort | awk "FNR == $1 {print}"
}

# get_shared_size <backup> <object> - the cluster-wide volume recorded in one of the two objects
get_shared_size() {
    wal-g --config=${TMP_CONFIG} st cat basebackups_005/$1/$2 | jq ".SharedSize"
}

count_shared_size_objects() {
    wal-g --config=${TMP_CONFIG} st ls basebackups_005/$1/ 2>&1 | grep -c _files_metadata.json || true
}

count_cluster_journals() {
    wal-g --config=${TMP_CONFIG} st ls basebackups_005/ 2>&1 | grep -c journal_ || true
}

bootstrap_gp_cluster
setup_wal_archiving

wal-g --config=${TMP_CONFIG} st rm / --target=all || true
wal-g --config=${TMP_CONFIG} delete everything FORCE --confirm

# The shared volume is recorded on every backup-push, with no flag of its own
insert_data
run_backup_logged ${TMP_CONFIG}
sleep 1

FIRST=$(get_backup_name 1)
test "0" -eq $(count_cluster_journals)
test "2" -eq $(count_shared_size_objects ${FIRST})

# insert_data creates an AO, a CO and a PAX table, so this backup pushed files to both aosegments/
# and paxfiles/. The two volumes stay in objects of their own instead of being summed into one.
test "0" -ne $(get_shared_size ${FIRST} ao_files_metadata.json)
test "0" -ne $(get_shared_size ${FIRST} pax_files_metadata.json)

# Permanent backups are counted too: they occupy the shared storage like any other
insert_data
run_backup_logged ${TMP_CONFIG} --permanent
sleep 1

PERMANENT=$(get_backup_name 2)
test "0" -ne $(get_shared_size ${PERMANENT} ao_files_metadata.json)
test "0" -ne $(get_shared_size ${PERMANENT} pax_files_metadata.json)

# The objects live under the backup name, so every delete mode removes them along with the backup
# without any code of its own
wal-g --config=${TMP_CONFIG} delete target ${FIRST} --confirm

test "0" -eq $(count_shared_size_objects ${FIRST})
test "2" -eq $(count_shared_size_objects ${PERMANENT})

cleanup
rm ${TMP_CONFIG}
