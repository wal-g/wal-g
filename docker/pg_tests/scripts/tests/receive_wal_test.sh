#!/bin/sh
set -e -x

. /tmp/tests/test_functions/prepare_config.sh
. /tmp/tests/test_functions/test_receive_wal.sh

prepare_config "/tmp/configs/receive_wal_test_config.json"
test_receive_wal ${TMP_CONFIG}
