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

# show client version
walg-daemon-client --version

MAXCLIENTSIZE=4194304 # 4MB
CLIENTSIZE=$(stat -c%s $(which walg-daemon-client))
# assert that resulting binary has the correct size
if [ $CLIENTSIZE -ge $MAXCLIENTSIZE ]; then
    echo "WAL-G daemon client size is too big ($CLIENTSIZE)"
    exit 1
else
    echo "WAL-G daemon client size OK ($CLIENTSIZE)"
fi

prepare_config "/tmp/configs/full_backup_test_config.json"
start_daemon
test_full_backup "${TMP_CONFIG}"
