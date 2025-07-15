#!/bin/bash
set -e -x

/home/gpadmin/run_greenplum.sh

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
