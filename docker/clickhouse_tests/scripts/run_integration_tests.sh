#!/bin/sh
set -e

for i in /tmp/tests/*; do
  echo
  echo "===== RUNNING $i ====="
  set -x
  "$i"
  set +x
  echo "===== SUCCESS $i ====="
  echo
done
