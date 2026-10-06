#!/bin/bash
set -e -x
. /tmp/tests/test_functions/prepare_config.sh
CONFIG_FILE="/tmp/configs/copy_source_config.json"
TO_CONFIG_FILE="/tmp/configs/copy_target_config.json"

TMP_CONFIG="/tmp/configs/tmp_config.json"
TO_TMP_CONFIG="/tmp/configs/to_tmp_config.json"
prepare_config "${CONFIG_FILE}" "${TMP_CONFIG}"
prepare_config "${TO_CONFIG_FILE}" "${TO_TMP_CONFIG}"
source /tmp/tests/test_functions/util.sh

count_objects() {
  wal-g st ls -r --config=$1 | grep -cE "$2" || true
}

COPY_LOG="/tmp/copy_test.log"

copy_backup() {
  wal-g copy --config=${TMP_CONFIG} --from=${TMP_CONFIG} --to=${TO_TMP_CONFIG} --backup-name=LATEST > ${COPY_LOG} 2>&1 \
    && EXIT_STATUS=$? || EXIT_STATUS=$?
  cat ${COPY_LOG}
  if [ "$EXIT_STATUS" -ne 0 ] ; then
    echo "Error: Failed to copy backup"
    exit 1
  fi
}

bootstrap_gp_cluster
setup_wal_archiving

wal-g --config=${TMP_CONFIG} delete everything FORCE --confirm
wal-g --config=${TO_TMP_CONFIG} delete everything FORCE --confirm

insert_data
run_backup_logged ${TMP_CONFIG} ${PGDATA} "--full"

psql -p 7000 -d test -c "INSERT INTO ao select i, i FROM generate_series(1,10)i;"
psql -p 7000 -d test -c "INSERT INTO co select i, i FROM generate_series(1,10)i;"
psql -p 7000 -d test -c "INSERT INTO pax_t select i, i FROM generate_series(1,10)i;"
run_backup_logged ${TMP_CONFIG} ${PGDATA}

wal-g --config=${TMP_CONFIG} backup-list
wal-g st ls -r --config=${TMP_CONFIG}

copy_backup
wal-g st ls -r --config=${TO_TMP_CONFIG}

for pattern in "aosegments/" "_D_[0-9]+_aoseg" "paxfiles/"; do
  source_count=$(count_objects ${TMP_CONFIG} "${pattern}")
  target_count=$(count_objects ${TO_TMP_CONFIG} "${pattern}")
  if [ "${source_count}" -eq 0 ] || [ "${source_count}" -ne "${target_count}" ]; then
    echo "Error: expected ${source_count} '${pattern}' objects in the copy, found ${target_count}"
    exit 1
  fi
done

copy_backup
if grep -q "Streamed" ${COPY_LOG}; then
  echo "Error: repeated copy wrote objects already present at the destination"
  exit 1
fi

stop_and_delete_cluster_dir
wal-g --config=${TMP_CONFIG} delete everything FORCE --confirm

wal-g backup-fetch LATEST --in-place --config=${TO_TMP_CONFIG}
prepare_cluster
start_cluster

for table in ao co pax_t; do
  psql -p 7000 -d test -c "SELECT COUNT(*) FROM ${table};" | grep -E "\b20\b" && EXIT_STATUS=$? || EXIT_STATUS=$?
  if [ "$EXIT_STATUS" -ne 0 ] ; then
      echo "Error: Failed to read from ${table} table after restore from the copy"
      exit 1
  fi
done

cleanup
rm ${TMP_CONFIG} ${TO_TMP_CONFIG} ${COPY_LOG}
