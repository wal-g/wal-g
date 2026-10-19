#!/bin/sh

wait_for_archived_wal() {
    archive_log=$1
    archive_deadline=$(($(date +%s) + $2))
    until grep -qi 'archived write-ahead log file' "$archive_log" 2>/dev/null; do
        if [ "$(date +%s)" -ge "$archive_deadline" ]; then
            echo "Timed out waiting for a successful WAL archive in $archive_log" >&2
            tail -n 200 "$archive_log" >&2 || true
            return 1
        fi
        sleep 1
    done
    grep -i 'archived write-ahead log file' "$archive_log"
}
