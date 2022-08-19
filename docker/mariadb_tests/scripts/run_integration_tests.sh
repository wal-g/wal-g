#!/bin/sh
set -e

. /usr/local/export_common.sh

for i in /tmp/tests/*; do
  echo
  echo "===== RUNNING $i ====="
  set -x
  timeout 3m "$i"
  set +x
  echo "===== SUCCESS $i ====="
  echo
  mariadb_kill_and_clean_data
done
