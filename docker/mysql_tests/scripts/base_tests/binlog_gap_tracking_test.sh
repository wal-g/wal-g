#!/bin/sh
set -e -x

. /usr/local/export_common.sh

export WALE_S3_PREFIX=s3://mysqlbinloggaptrackingbucket
export WALG_MYSQL_BINLOG_GAP_TRACKING=False

sentinel=/tmp/binlog_gap_tracking_sentinel.json
next_transaction=1

# Put one transaction in each file and rotate it so it can be uploaded.
generate_binlogs() {
    binlogs_to_create=$1
    while [ "$binlogs_to_create" -gt 0 ]; do
        last_generated_binlog=$(mysql_current_binlog)
        mysql -e "INSERT INTO sbtest.binlog_gaps VALUES ($next_transaction); FLUSH BINARY LOGS"
        next_transaction=$((next_transaction + 1))
        binlogs_to_create=$((binlogs_to_create - 1))
    done
}

push_binlogs() {
    wal-g binlog-push
    wal-g st cat binlog_sentinel_005.json > "$sentinel"
    cat "$sentinel"
}

assert_archived_gtids() {
    jq -e --arg archived "$1" --arg last_binlog "$last_generated_binlog" '
        .GtidArchived == $archived and .LastUploadedBinlog == $last_binlog
    ' "$sentinel"
}

mysql_initialize_and_start
mysql -e "CREATE TABLE sbtest.binlog_gaps (id INT PRIMARY KEY)"
# Keep the table but reset GTIDs so transaction numbers are deterministic on every MySQL version.
mysql_reset_binary_logs_and_gtids
server_uuid=$(mysql --batch --skip-column-names -e "SELECT @@GLOBAL.server_uuid")

# Upload transactions 1-2 with gap tracking disabled.
generate_binlogs 2

push_binlogs

assert_archived_gtids "${server_uuid}:1-2"
jq -e 'has("GapTracking") | not' "$sentinel"

# Purge transaction 3 before uploading it, then upload transactions 4-5 with tracking still disabled.
generate_binlogs 1
current_binlog=$(mysql_current_binlog)
mysql -e "PURGE BINARY LOGS TO '$current_binlog'"
generate_binlogs 2

push_binlogs

assert_archived_gtids "${server_uuid}:1-2:4-5"
jq -e 'has("GapTracking") | not' "$sentinel"

# Enable tracking and upload transactions 6-7: the existing gap has no known location.
export WALG_MYSQL_BINLOG_GAP_TRACKING=True
generate_binlogs 2

push_binlogs

assert_archived_gtids "${server_uuid}:1-2:4-7"
jq -e --arg gap "${server_uuid}:3" '
    .GapTracking == {GapWithoutLocation: $gap}
' "$sentinel"
last_uploaded_before_gap=$last_generated_binlog

# Purge transactions 8-9, then upload 10-11: this gap follows the last uploaded binlog.
generate_binlogs 2
current_binlog=$(mysql_current_binlog)
mysql -e "PURGE BINARY LOGS TO '$current_binlog'"
generate_binlogs 2

push_binlogs

assert_archived_gtids "${server_uuid}:1-2:4-7:10-11"
jq -e --arg unknown_gap "${server_uuid}:3" --arg located_gap "${server_uuid}:8-9" \
    --arg replay_stop "$last_uploaded_before_gap" '
    .GapTracking == {
        GapWithoutLocation: $unknown_gap,
        Gaps: [{GtidSet: $located_gap, ReplayStopBinlog: $replay_stop}]
    }
' "$sentinel"

# Disable tracking and upload transactions 12-13: clear gaps while keeping the archived checkpoint.
export WALG_MYSQL_BINLOG_GAP_TRACKING=False
generate_binlogs 2

push_binlogs

assert_archived_gtids "${server_uuid}:1-2:4-7:10-13"
jq -e 'has("GapTracking") | not' "$sentinel"
