#!/bin/sh

archive_conf() {
  echo "archive_mode = on"
  echo "archive_command = '/usr/bin/timeout 600 wal-g --config=${TMP_CONFIG} wal-push %p'"
  echo "archive_timeout = 600"
}

recovery_conf() {
  echo "restore_command = 'echo \"WAL file restoration: %f, %p\"&& wal-g --config=${TMP_CONFIG} wal-fetch \"%f\" \"%p\"'"
}

drop_pg() {
  /tmp/scripts/drop_pg.sh
}

dump_and_bench() {
  pg_dumpall -f /tmp/dump1
  pgbench -c 2 -T 100000000 -S &
}

test_wal_overwrites() {
  # Also we test here WAL overwrite prevention as a part of regular backup functionality
  # First test that .history files prevent overwrite even if WALG_PREVENT_WAL_OVERWRITE is false
  export WALG_PREVENT_WAL_OVERWRITE=false

  echo test > ${PGDATA}/pg_wal/test_file.history
  wal-g --config=${TMP_CONFIG} wal-push ${PGDATA}/pg_wal/test_file.history
  wal-g --config=${TMP_CONFIG} wal-push ${PGDATA}/pg_wal/test_file.history

  echo test1 > ${PGDATA}/pg_wal/test_file.history
  wal-g --config=${TMP_CONFIG} wal-push ${PGDATA}/pg_wal/test_file && EXIT_STATUS=$? || EXIT_STATUS=$?

  if [ "$EXIT_STATUS" -eq 0 ] ; then
      echo "Error: Duplicate .history with different content was pushed"
      exit 1
  fi

  export WALG_PREVENT_WAL_OVERWRITE=true

  echo test > ${PGDATA}/pg_wal/test_file
  wal-g --config=${TMP_CONFIG} wal-push ${PGDATA}/pg_wal/test_file
  wal-g --config=${TMP_CONFIG} wal-push ${PGDATA}/pg_wal/test_file

  echo test1 > ${PGDATA}/pg_wal/test_file
  wal-g --config=${TMP_CONFIG} wal-push ${PGDATA}/pg_wal/test_file && EXIT_STATUS=$? || EXIT_STATUS=$?

  if [ "$EXIT_STATUS" -eq 0 ] ; then
      echo "Error: Duplicate WAL with different content was pushed"
      exit 1
  fi
}

test_full_backup()
{
  TMP_CONFIG=$1
  /usr/lib/postgresql/10/bin/initdb ${PGDATA}

  PG_VERSION=$(cat "${PGDATA}/PG_VERSION")
  archive_conf >> ${PGDATA}/postgresql.conf

  /usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start

  /tmp/scripts/wait_while_pg_not_ready.sh

  wal-g --config=${TMP_CONFIG} delete everything FORCE --confirm

  pgbench -i -s 5 postgres

  if [ -z "${FORCE_NEW_WAL}" ]; then
    dump_and_bench
  fi
  sleep 1
  wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}

  if [ -n "${FORCE_NEW_WAL}" ]; then
    echo force creating new WAL files
    pgbench -i -s 5 postgres
    dump_and_bench

    echo transporting last wal files
    if awk 'BEGIN {exit !('"$PG_VERSION"' >= 10)}'; then
      echo 'select pg_switch_wal();' | psql
    else
      echo 'select pg_switch_xlog();' | psql
    fi
    sleep 2
  fi

  drop_pg

  wal-g --config=${TMP_CONFIG} backup-fetch ${PGDATA} LATEST

  # https://www.postgresql.org/docs/current/recovery-config.html
  if awk 'BEGIN {exit !('"$PG_VERSION"' >= 12)}'; then
    touch "$PGDATA/recovery.signal"
    recovery_conf >> "$PGDATA/postgresql.conf"
  else
    recovery_conf > "$PGDATA/recovery.conf"
  fi

  /usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start
  /tmp/scripts/wait_while_pg_not_ready.sh
  pg_dumpall -f /tmp/dump2

  diff /tmp/dump1 /tmp/dump2

  psql -f /tmp/scripts/amcheck.sql -v "ON_ERROR_STOP=1" postgres

  echo "Full backup success!!!!!!"

  if [ -z "${SKIP_TEST_WAL_OVERWRITES}" ]; then
    test_wal_overwrites
  else
    echo "test_wal_overwrites skipped"
  fi

  drop_pg
  rm ${TMP_CONFIG}

  echo "Prevent WAL overwrite success!!!!!!"
}
