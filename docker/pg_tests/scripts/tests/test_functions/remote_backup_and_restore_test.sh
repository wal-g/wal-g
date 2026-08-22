#!/bin/sh
set -e

. /tmp/tests/test_functions/pg_compat.sh

recovery_conf() {
  echo "recovery_target_action=promote"
  echo "restore_command='wal-g --config=${TMP_CONFIG} wal-fetch %f %p'"
}

remote_backup_and_restore_test() {
  TMP_CONFIG=$1
  TMPDIR=${TMPDIR:-$(mktemp -d)}
  echo "All data can be found in $TMPDIR"
  PGTBS="$(dirname "${PGDATA}")/tbs"
  export PGTBS
  mkdir "${PGTBS}"

  echo Initializing source
  initdb
  echo "local replication postgres trust" >> "$PGDATA/pg_hba.conf"
  echo "archive_command = 'wal-g --config=${TMP_CONFIG} wal-push %p'
  archive_mode = on
  logging_collector=on
  wal_level=replica
  max_wal_senders=5" >> "$PGDATA/postgresql.conf"

  echo Starting source
  pg_ctl start
  /tmp/scripts/wait_while_pg_not_ready.sh

  psql -c 'select version();'

  echo Loading random data to source
  pgbench -i -s 10 -h 127.0.0.1 -p 5432 postgres

  echo "Dumping source"
  dump_db "${TMPDIR}/srcdump.sql"

  echo Backup source
  wal-g --config=${TMP_CONFIG} backup-push

  echo transporting last wal files
  switch_wal

  echo PGLog
  cat $PGDATA/log/*

  echo Stopping source
  pg_ctl stop
  rm -rf "${PGTBS}"/*
  rm -rf "${PGDATA}"

  echo Debug
  wal-g --config=${TMP_CONFIG} st ls -r

  echo Restore destination
  BACKUP=$(wal-g --config=${TMP_CONFIG} backup-list | sed -n '2{s/ .*//;p}')
  wal-g --config=${TMP_CONFIG} backup-fetch "$PGDATA" "$BACKUP"
  chmod 0700 "$PGDATA"
  setup_recovery_settings recovery_conf

  echo Starting destination
  pg_ctl start || { cat $PGDATA/log/* ; exit 1 ; }
  /tmp/scripts/wait_while_pg_not_ready.sh

  echo "Dumping destination"
  dump_db "${TMPDIR}/dstdump.sql"

  echo PGLog
  cat $PGDATA/log/*

  echo Stopping destination
  pg_ctl stop

  echo Comparing source and destination
  if compare_dumps "${TMPDIR}/srcdump.sql" "${TMPDIR}/dstdump.sql"; then
    /tmp/scripts/drop_pg.sh
    rm ${TMP_CONFIG}
    rm -rf ${PGTBS}
    echo OK
  else
    echo Ouch
    return 1
  fi
}

PGBIN=$(ls -d /usr/lib/postgresql/*/bin | xargs -n 1)
export PATH=$PGBIN:$PATH
