#!/bin/sh
set -e -x

. /tmp/tests/test_functions/pg_compat.sh
# The Go fixtures use standby.signal (PostgreSQL 12+). Keep these tests in
# every supported modern PostgreSQL image, alongside the normal unit CI.
require_pg_ge 14

export WALG_TEST_PG_BIN="/usr/lib/postgresql/${PG_MAJOR}/bin"
cd /tmp/test_bins/internal/databases/postgres
/tmp/test_bins/postgres.test \
  -test.run='^TestWalSend(StockReceivewal(Live)?TimelineSwitch|RealPostgresStandby)$' \
  -test.v -test.timeout=180s
