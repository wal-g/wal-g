#!/bin/sh
set -e

. /usr/local/export_common.sh

prefix=/tmp/tests

while getopts ":p:" o; do
    case "${o}" in
        p)
            prefix=${OPTARG}
            ;;
        *)
            usage
            ;;
    esac
done

# to cleanup from previous possibly unsuccessful tests run
mysql_kill_and_clean_data

for i in "$prefix"/*; do
  echo
  echo "===== RUNNING $i ====="
  set -x
  chmod a+x "$i"
  timeout 3m "$i"
  set +x
  echo "===== SUCCESS $i ====="
  echo
  mysql_kill_and_clean_data
done
