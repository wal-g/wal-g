#!/bin/bash
set -e -x

/home/gpadmin/run_greenplum.sh

pushd /tmp
for i in tests/*.sh; do
  if [ "$i" != "tests/partial_table_restore_test.sh" ]; then continue ;fi

  echo
  echo "===== RUNNING $i ====="
  set -x
  ./"$i";

  set +x
  echo "===== SUCCESS $i ====="
  echo
done
popd
