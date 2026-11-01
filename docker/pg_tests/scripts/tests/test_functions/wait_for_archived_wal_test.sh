#!/bin/sh
set -eu
. "$(dirname -- "$0")/wait_for_archived_wal.sh"
test_dir=$(mktemp -d)
trap 'rm -f "$test_dir/archive.log"; rmdir "$test_dir"' EXIT

# A success message passes immediately, even at a zero-second deadline.
printf '%s\n' 'DEBUG: archived write-ahead log file "0001"' > "$test_dir/archive.log"
wait_for_archived_wal "$test_dir/archive.log" 0

# A missing log and a log containing only errors must not pass.
if wait_for_archived_wal "$test_dir/missing.log" 0; then exit 1; fi
printf '%s\n' 'ERROR: archive command failed' > "$test_dir/archive.log"
if wait_for_archived_wal "$test_dir/archive.log" 0; then exit 1; fi

# Wait for the condition instead of requiring success on the first check.
(sleep 1; printf '%s\n' 'DEBUG: archived write-ahead log file "0002"' >> "$test_dir/archive.log") &
writer=$!
wait_for_archived_wal "$test_dir/archive.log" 5
wait "$writer"
echo 'Archive wait checks passed'
