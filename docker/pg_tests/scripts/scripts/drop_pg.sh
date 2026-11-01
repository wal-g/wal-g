#!/bin/bash
pkill -9 postgres || true
pkill -9 wal-g || true
# This disposable test repository belongs to the cluster being discarded.
# Drop its reservations too, before another initdb reuses the same WAL names.
rm -rf $PGDATA /tmp/basebackups_005 /tmp/backup_reservations /tmp/wal_005 /tmp/spaces /tmp/spaces_backup
