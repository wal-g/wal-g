#!/bin/bash
set -e -x

for i in tests/*; do
  echo
  echo "===== RUNNING $i ====="
  set -x
  ./"$i";

  set +x
  echo "===== SUCCESS $i ====="
  echo
done
popd