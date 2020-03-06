#!/bin/sh
set -e

. /usr/local/export_common.sh

for i in /tmp/tests/*; do
  echo
  echo "===== RUNNING $i ====="
  set -x
  "$i"
  set +x
  echo "===== SUCCESS $i ====="
  echo
  mysql_kill_and_clean_data
done
