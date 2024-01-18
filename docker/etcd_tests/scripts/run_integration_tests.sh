#!/bin/bash
set -e -x

. /usr/local/export_common.sh
unset ETCD_DATA_DIR

for i in /tmp/tests/*.sh; do
  echo
  echo "===== RUNNING $i ====="
  set -x
  bash "$i";
  set +x
  echo "===== SUCCESS $i ====="
  etcd_kill_and_clean_data
  echo
done