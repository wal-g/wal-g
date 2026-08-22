#!/bin/sh

. /tmp/tests/test_functions/pg_compat.sh

test_copy_composer()
{
  TMP_CONFIG=$1
  initdb ${PGDATA}

  echo "archive_mode = on" >> ${PGDATA}/postgresql.conf
  echo "archive_command = '/usr/bin/timeout 600 wal-g --config=${TMP_CONFIG} wal-push %p'" >> ${PGDATA}/postgresql.conf
  echo "archive_timeout = 600" >> ${PGDATA}/postgresql.conf

  pg_ctl -D ${PGDATA} -w start

  wal-g --config=${TMP_CONFIG} delete everything FORCE --confirm

  pgbench -i -s 100

  wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}

  pgbench

  dump_all /tmp/dump1

  wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}

  /tmp/scripts/drop_pg.sh

  wal-g --config=${TMP_CONFIG} backup-fetch ${PGDATA} LATEST

  echo "restore_command = 'echo \"WAL file restoration: %f, %p\"&& wal-g --config=${TMP_CONFIG} wal-fetch \"%f\" \"%p\"'" | write_recovery_settings

  pg_ctl -D ${PGDATA} -w start
  /tmp/scripts/wait_while_pg_not_ready.sh
  dump_all /tmp/dump2

  compare_dumps /tmp/dump1 /tmp/dump2

  psql -f /tmp/scripts/amcheck.sql -v "ON_ERROR_STOP=1" postgres

  echo "Full backup with copy-composer success!!!!!!"
}
