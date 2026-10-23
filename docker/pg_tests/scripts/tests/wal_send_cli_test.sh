#!/bin/sh
set -e -x

. /tmp/tests/test_functions/prepare_config.sh
prepare_config /tmp/configs/wal_send_test_config.json /tmp/configs/wal_send_cli.json
trap 'rm -f /tmp/configs/wal_send_cli.json' EXIT

export WALG_TEST_PG_BIN="/usr/lib/postgresql/${PG_MAJOR}/bin"
export WALG_TEST_WALG_BIN=/usr/bin/wal-g
export WALG_TEST_CLI_CONFIG=/tmp/configs/wal_send_cli.json
cd /tmp/test_bins/internal/databases/postgres
/tmp/test_bins/postgres.test -test.run='^TestWalSendCLIStandby(64MB)?$' -test.v -test.timeout=300s
