#!/bin/sh
set -e -x

CONFIG_FILE="/tmp/configs/wal_perftest_config.json"

COMMON_CONFIG="/tmp/configs/common_config.json"
TMP_CONFIG="/tmp/configs/tmp_config.json"
cat ${CONFIG_FILE} > ${TMP_CONFIG}

echo "," >> ${TMP_CONFIG}
cat ${COMMON_CONFIG} >> ${TMP_CONFIG}
/tmp/scripts/wrap_config_file.sh ${TMP_CONFIG}

WAL=$(ls -l ${PGDATA}/pg_wal | head -n2 | tail -n1 | egrep -o "[0-9A-F]{24}")

wal-g --config=${TMP_CONFIG} daemon "${PGDATA}"/pg_wal

# shellcheck disable=SC2039
nc -U /tmp/wal-push.sock <<< 'CHECK'
sleep 1

# shellcheck disable=SC2039
nc -U /tmp/wal-push.sock <<< "${WAL}"
sleep 1

wal-g st ls WALG_S3_PREFIX/wal_005/"${WAL}".br


