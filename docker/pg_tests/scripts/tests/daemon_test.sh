#!/bin/bash
set -e -x

CONFIG_FILE="/tmp/configs/daemon_test_config.json"

COMMON_CONFIG="/tmp/configs/common_config.json"
TMP_CONFIG="/tmp/configs/tmp_config.json"
cat ${CONFIG_FILE} > ${TMP_CONFIG}

echo "," >> ${TMP_CONFIG}
cat ${COMMON_CONFIG} >> ${TMP_CONFIG}
/tmp/scripts/wrap_config_file.sh ${TMP_CONFIG}

WAL=$(ls -l ${PGDATA}/pg_wal | head -n2 | tail -n1 | egrep -o "[0-9A-F]{24}")
SOCKET="/tmp/wal-daemon.sock"

wal-g --config=${TMP_CONFIG} daemon ${SOCKET}

if (echo -en "C\x0\x8"; echo -n "CHECK"; echo -en "F\x0\x1B"; echo -n "${WAL}") | nc -U ${SOCKET} | grep -q "OO"; then
  echo "WAL-G response is correct"
else
  echo "Error in WAL-G response"
  exit 1
fi

wal-g st ls ${WALE_S3_PREFIX}/wal_005/${WAL}.br
