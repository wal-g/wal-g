#!/bin/sh
set -e -x

./tmp/full_backup_test.sh
./tmp/delta_backup_fullscan_test.sh
./tmp/delta_backup_wal_delta_test.sh
./tmp/wale_compatibility_test.sh
./tmp/several_delta_backups_test.sh
./tmp/ghost_table_test.sh
