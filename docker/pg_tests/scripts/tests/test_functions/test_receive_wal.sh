#!/bin/sh
set -e
test_receive_wal()
{
  TMP_CONFIG=$1
  /usr/lib/postgresql/10/bin/initdb ${PGDATA}

  /usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start

  /tmp/scripts/wait_while_pg_not_ready.sh

  wal-g --config=${TMP_CONFIG} wal-receive &

  pgbench -i -s 5 postgres
  pg_dumpall -f /tmp/dump1
  pgbench -c 2 -T 10 -S
  sleep 1
  VERIFY_OUTPUT=$(mktemp)
  # Verify and store in temp file
  wal-g --config=${TMP_CONFIG} wal-verify integrity > "${VERIFY_OUTPUT}"
  /usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w stop -m immediate

  # parse verify results
  VERIFY_RESULT=$(awk 'BEGIN{FS=":"}$1~/integrity check status/{print $2}' $VERIFY_OUTPUT)

  cat "${VERIFY_OUTPUT}"

  # check verify results to end with 'OK'
  if echo "$VERIFY_RESULT" | grep -qP "\bOK$"; then
    echo "WAL receive success!!!!!!"
    return 0
  fi
  echo "WAL not received as expected!!!!!"
  return 1
}
