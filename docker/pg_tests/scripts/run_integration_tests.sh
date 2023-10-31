#!/bin/bash
set -e -x -v

pushd /tmp

#while true
#do
#    sleep 10
#done

for i in tests/*.sh; do
  # Exclude pgbackrest because it needs to be run in separate container. It should be run individually.
  if [ "$i" = "tests/pgbackrest_backup_fetch_test.sh" ]; then continue ;fi
  # Exclude ssh test because of ssh server container dependency.
  if [ "$i" = "tests/ssh_backup_test.sh" ]; then continue ;fi
  if [ "$i" != "tests/zzz.sh" ]; then continue ;fi
  echo
  echo "===== RUNNING $i ====="
  set -x
  
  ./"$i";

  set +x
  echo "===== SUCCESS $i ====="
  echo

while true
do
    sleep 10
done
done
popd
