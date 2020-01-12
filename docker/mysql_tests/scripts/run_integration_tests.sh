#!/bin/sh
set -e -x

for i in /tmp/tests/*; do
  "$i";
  echo "${i} success"
  kill_mysql_and_cleanup_data
  rm -rf /root/.walg_mysql_binlogs_cache
done
