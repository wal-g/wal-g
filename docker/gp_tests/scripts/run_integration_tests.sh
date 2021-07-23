#!/bin/bash
set -e -x

pushd /tmp
for i in tests/*.sh; do
  echo
  echo "===== RUNNING $i ====="
  set -x
  ./"$i";

  set +x
  echo "===== SUCCESS $i ====="
  echo
done
popd
