#!/bin/bash
set -e -x
CONFIG_FILE="/tmp/configs/journals_test_config.json"

COMMON_CONFIG="/tmp/configs/common_config.json"
TMP_CONFIG="/tmp/configs/tmp_config.json"
cat ${CONFIG_FILE} > ${TMP_CONFIG}
echo "," >> ${TMP_CONFIG}
cat ${COMMON_CONFIG} >> ${TMP_CONFIG}
/tmp/pg_scripts/wrap_config_file.sh ${TMP_CONFIG}
source /tmp/tests/test_functions/util.sh

# The names of the cluster-wide journals, oldest first. One per cluster backup.
get_cluster_journals() {
    wal-g --config=${TMP_CONFIG} st ls basebackups_005/ 2>&1 | grep journal_ | awk '{ print $NF }' | sort
}

get_cluster_journal_count() {
    get_cluster_journals | wc -l
}

# get_backup_name <n> - name of the n-th oldest backup, derived from its journal name
get_backup_name() {
    JOURNAL_NAME=$(get_cluster_journals | awk "FNR == $1 {print}")
    echo ${JOURNAL_NAME#"journal_"}
}

# get_cluster_journal_field <n> <field> - journal field recorded for the n-th oldest backup.
# Absent fields (SharedSize is omitempty) read as 0.
get_cluster_journal_field() {
    JOURNAL_NAME=$(get_cluster_journals | awk "FNR == $1 {print}")
    wal-g --config=${TMP_CONFIG} st cat basebackups_005/$JOURNAL_NAME | jq ".$2 // 0"
}

# get_cluster_journal_size <n> - SizeToNextBackup recorded for the n-th oldest backup
get_cluster_journal_size() {
    get_cluster_journal_field $1 SizeToNextBackup
}

count_segment_journals() {
    wal-g --config=${TMP_CONFIG} st ls segments_005/seg$1/basebackups_005/ 2>&1 | grep journal_ | wc -l
}

count_shared_objects() {
    wal-g --config=${TMP_CONFIG} st ls -r 2>&1 | grep -c "$1" || true
}

bootstrap_gp_cluster
setup_wal_archiving

wal-g --config=${TMP_CONFIG} st rm / --target=all || true
wal-g --config=${TMP_CONFIG} delete everything FORCE --confirm

for i in 1 2 3
do
    insert_data
    run_backup_logged ${TMP_CONFIG} --count-journals
    sleep 1
done

# Every backup got a cluster-wide journal, and every segment got its own one
test "3" -eq $(get_cluster_journal_count)
for CONTENT_ID in -1 0 1 2
do
    test "3" -eq $(count_segment_journals ${CONTENT_ID})
done

# The newest backup has nothing after it yet, the older ones accumulated some WAL
FIRST_SIZE=$(get_cluster_journal_size 1)
SECOND_SIZE=$(get_cluster_journal_size 2)
test "0" -ne $FIRST_SIZE
test "0" -ne $SECOND_SIZE
test "0" -eq $(get_cluster_journal_size 3)

# On Cloudberry the shared volume of a backup is the AO/AOCS files it pushed to aosegments/ plus the
# PAX files it pushed to paxfiles/. insert_data creates an AO, a CO and a PAX table, so every backup
# added something to both prefixes and none of the three journals may report a zero shared volume.
test "0" -ne $(count_shared_objects "aosegments/")
test "0" -ne $(count_shared_objects "paxfiles/")
for N in 1 2 3
do
    test "0" -ne $(get_cluster_journal_field $N SharedSize)
done

# Deleting a backup in the middle merges its interval into the previous one
wal-g --config=${TMP_CONFIG} delete target $(get_backup_name 2) --confirm

test "2" -eq $(get_cluster_journal_count)
for CONTENT_ID in -1 0 1 2
do
    test "2" -eq $(count_segment_journals ${CONTENT_ID})
done

test $((FIRST_SIZE + SECOND_SIZE)) -eq $(get_cluster_journal_size 1)
test "0" -eq $(get_cluster_journal_size 2)

# The shared volume belongs to the backup itself rather than to the interval after it, so the
# surviving backups keep reporting their own AO/AOCS/PAX volume after the merge.
for N in 1 2
do
    test "0" -ne $(get_cluster_journal_field $N SharedSize)
done

# Deleting the newest backup leaves the remaining one as the newest
wal-g --config=${TMP_CONFIG} delete target $(get_backup_name 2) --confirm

test "1" -eq $(get_cluster_journal_count)
test "0" -eq $(get_cluster_journal_size 1)

# Deleting the last remaining backup removes its journals everywhere
wal-g --config=${TMP_CONFIG} delete target $(get_backup_name 1) --confirm

test "0" -eq $(get_cluster_journal_count)
for CONTENT_ID in -1 0 1 2
do
    test "0" -eq $(count_segment_journals ${CONTENT_ID})
done

cleanup
rm ${TMP_CONFIG}
