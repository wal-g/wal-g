#!/bin/bash
set -e

backup_agent -C /var/fdb/fdb.cluster &

for test_name in $(dirname $BASH_SOURCE | xargs realpath)/tests/*; do
  echo
  echo "===== RUNNING $test_name ====="
  set -x
  "$test_name"
  set +x
  echo "===== SUCCESS $test_name ====="
  echo
done
