#!/bin/bash
set -e -x

for i in /tmp/tests/*; do
  echo
  echo "===== RUNNING $i ====="
  set -x
  bash "$i";

  set +x
  echo "===== SUCCESS $i ====="
  echo
done