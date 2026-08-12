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

# The names of the cluster-wide journals, oldest first. One per gp backup.
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

# get_segments_journal_sum <n> <field> - sum of the field over the segment journals of the
# n-th oldest backup, i.e. what the cluster-wide journal is supposed to hold
get_segments_journal_sum() {
    BACKUP_NAME=$(get_backup_name $1)
    FIELD=${2:-SizeToNextBackup}
    SENTINEL=$(wal-g --config=${TMP_CONFIG} st cat basebackups_005/${BACKUP_NAME}_backup_stop_sentinel.json)

    SUM=0
    for SEGMENT in $(echo "$SENTINEL" | jq -r '.segments[] | "\(.content_id):\(.backup_name)"'); do
        CONTENT_ID=${SEGMENT%%:*}
        SEG_BACKUP=${SEGMENT#*:}
        SEG_SIZE=$(wal-g --config=${TMP_CONFIG} st cat \
            segments_005/seg${CONTENT_ID}/basebackups_005/journal_${SEG_BACKUP} | jq ".$FIELD // 0")
        SUM=$((SUM + SEG_SIZE))
    done
    echo $SUM
}

count_segment_journals() {
    wal-g --config=${TMP_CONFIG} st ls segments_005/seg$1/basebackups_005/ 2>&1 | grep journal_ | wc -l
}

bootstrap_gp_cluster
setup_wal_archiving
enable_pitr_extension

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

# The cluster-wide size is exactly the sum over the segments
test $FIRST_SIZE -eq $(get_segments_journal_sum 1 SizeToNextBackup)
test $SECOND_SIZE -eq $(get_segments_journal_sum 2 SizeToNextBackup)

# insert_data creates AO and CO tables, so every backup pushed some AO/AOCS files to the shared
# storage, and the cluster-wide figure is again the sum over the segments.
for N in 1 2 3
do
    SHARED_SIZE=$(get_cluster_journal_field $N SharedSize)
    test "0" -ne $SHARED_SIZE
    test $SHARED_SIZE -eq $(get_segments_journal_sum $N SharedSize)
done

# Deleting a backup in the middle merges its interval into the previous one
wal-g --config=${TMP_CONFIG} delete target $(get_backup_name 2) --confirm

test "2" -eq $(get_cluster_journal_count)
for CONTENT_ID in -1 0 1 2
do
    test "2" -eq $(count_segment_journals ${CONTENT_ID})
done

MERGED_SIZE=$(get_cluster_journal_size 1)
test $MERGED_SIZE -eq $((FIRST_SIZE + SECOND_SIZE))
test $MERGED_SIZE -eq $(get_segments_journal_sum 1)
test "0" -eq $(get_cluster_journal_size 2)

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
