#!/bin/sh
set -e

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
  PGVERSION=$(cat "${PGDATA}/PG_VERSION")
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
  pg_dump > "${TMPDIR}/srcdump.sql"

  echo Backup source
  wal-g --config=${TMP_CONFIG} backup-push

  echo transporting last wal files
  if awk 'BEGIN {exit !('"$PGVERSION"' >= 10)}'; then
    echo 'select pg_switch_wal();' | psql
  else
    echo 'select pg_switch_xlog();' | psql
  fi

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
  if awk 'BEGIN {exit !('"$PGVERSION"' >= 12)}'; then
    touch "$PGDATA/recovery.signal"
    recovery_conf >> "$PGDATA/postgresql.conf"
  else
    recovery_conf > "$PGDATA/recovery.conf"
  fi

  echo Starting destination
  pg_ctl start || { cat $PGDATA/log/* ; exit 1 ; }
  /tmp/scripts/wait_while_pg_not_ready.sh

  echo "Dumping destination"
  pg_dump > "${TMPDIR}/dstdump.sql"

  echo PGLog
  cat $PGDATA/log/*

  echo Stopping destination
  pg_ctl stop

  echo Comparing source and destination
  if diff "${TMPDIR}"/*dump.sql; then
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
