#!/bin/sh
set -e -x

. /tmp/tests/test_functions/prepare_config.sh
. /tmp/tests/test_functions/test_full_backup.sh

prepare_config "/tmp/configs/full_backup_failover_storages_test_config.json"
test_full_backup ${TMP_CONFIG}
