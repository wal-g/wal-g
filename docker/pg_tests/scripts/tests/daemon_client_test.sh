#!/bin/sh
set -e -x

export PGDATA=/var/lib/postgresql/10/main
export WALG_SOCKET="/tmp/wal-daemon.sock"
export FORCE_NEW_WAL="1"
export SKIP_TEST_WAL_OVERWRITES="1"

# load functions
. /tmp/tests/test_functions/prepare_config.sh
. /tmp/tests/test_functions/test_full_backup.sh
. /tmp/tests/test_functions/daemon_patch.sh
. /tmp/tests/test_functions/daemon_client_patch.sh

prepare_config "/tmp/configs/full_backup_test_config.json"
start_daemon
test_full_backup "${TMP_CONFIG}"
