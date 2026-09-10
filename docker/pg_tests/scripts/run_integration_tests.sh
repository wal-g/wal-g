#!/bin/bash
set -e -x

pushd /tmp

for i in tests/*.sh; do
  # Exclude pgbackrest because it needs to be run in separate container. It should be run individually.
  if [ "$i" = "tests/pgbackrest_backup_fetch_test.sh" ]; then continue ;fi
  # Exclude ssh test because of ssh server container dependency.
  if [ "$i" = "tests/ssh_backup_test.sh" ]; then continue ;fi
  # Exclude throttling test because of different s3 container for throttling.
  if [ "$i" = "tests/wal_perftest_with_throttling.sh" ]; then continue ;fi
  echo
  echo "===== RUNNING $i ====="
  set -x

  # A test that does not apply to this PostgreSQL version exits with 77, see
  # pg_compat.sh.
  set +e
  ./"$i"
  status=$?
  set -e

  set +x
  case $status in
    0)  echo "===== SUCCESS $i =====" ;;
    77) echo "===== SKIPPED $i =====" ;;
    *)  echo "===== FAILED  $i (exit $status) ====="; exit $status ;;
  esac
  echo

done
popd
