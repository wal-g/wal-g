#!/bin/bash
set -e -x

/home/gpadmin/run_greenplum.sh

pushd /tmp
for i in tests/*.sh; do
  echo
  echo "=============================="
  echo "===== RUNNING $i ====="
  echo "=============================="
  set -x
  ./"$i";

  set +x
  echo "=============================="
  echo "===== SUCCESS $i ====="
  echo "=============================="
  echo
done
popd
