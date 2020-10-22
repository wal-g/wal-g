#!/bin/sh
set -e

. /usr/local/export_common.sh

# to cleanup from previous possibly unsuccessful tests run
mysql_kill_and_clean_data

/tmp/tests/copy_between_different_accounts_storage.sh
mysql_kill_and_clean_data

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
