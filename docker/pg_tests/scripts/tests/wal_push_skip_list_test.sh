#!/bin/sh
set -e -x

export WALG_TEST_WALG_BIN=/usr/bin/wal-g
cd /tmp/test_bins/internal/databases/postgres
/tmp/test_bins/postgres.test -test.run='^TestWalPushCLISkipList$' -test.v -test.timeout=60s
