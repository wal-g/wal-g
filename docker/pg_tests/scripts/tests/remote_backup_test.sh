#!/bin/sh
set -e -x

. /tmp/tests/test_functions/prepare_config.sh
. /tmp/tests/test_functions/remote_backup_and_restore_test.sh

prepare_config "/tmp/configs/remote_backup_test_config.json"
remote_backup_and_restore_test "${TMP_CONFIG}"
