#!/bin/bash
set -e -x

. /tmp/tests/test_functions/prepare_config.sh
prepare_config "/tmp/configs/daemon_test_config.json"

initdb ${PGDATA}
pg_ctl -D ${PGDATA} -w start

wal-g --config=${TMP_CONFIG} delete everything FORCE --confirm

pgbench -i -s 50 postgres
du -hs ${PGDATA}
sleep 1
WAL=$(ls -l ${PGDATA}/pg_wal | head -n2 | tail -n1 | egrep -o "[0-9A-F]{24}")

SOCKET="/tmp/configs/wal-daemon.sock"
wal-g --config=${TMP_CONFIG} daemon ${SOCKET} &

until [ -S ${SOCKET} ]
do
  sleep 1
done
echo "walg-daemon is working"

if {
  echo -en "C\x0\x8"
  echo -n "CHECK"
  echo -en "F\x0\x1B"
  echo -n "${WAL}"
} | nc -U ${SOCKET} | grep -q "OO"; then
  echo "WAL-G response is correct"
  if wal-g --config=${TMP_CONFIG} st ls /wal_005 | grep ${WAL}.br ; then
      echo "Archive file in folder"
  else
    echo "Archive not in folder. Error."
    exit 1
  fi
else
  echo "Error in WAL-G response."
  exit 1
fi

/tmp/scripts/drop_pg.sh
