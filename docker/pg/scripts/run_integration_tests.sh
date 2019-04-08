#!/bin/sh
set -e -x

./tmp/full_backup_test.sh
./tmp/delete_end_to_end_test.sh
./tmp/delete_before_name_find_full_test.sh
./tmp/delete_before_time_find_full_test.sh
./tmp/delete_retain_find_full_test.sh
./tmp/delete_retain_full_test.sh
./tmp/delete_without_confirm_test.sh
./tmp/delta_backup_fullscan_test.sh
./tmp/delta_backup_wal_delta_test.sh
./tmp/wale_compatibility_test.sh
./tmp/several_delta_backups_test.sh
./tmp/ghost_table_test.sh
