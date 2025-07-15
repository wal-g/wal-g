#!/bin/bash
set -e -x

. /usr/local/export_common.sh

etcd --data-dir $WALG_ETCD_DATA_DIR &

etcdctl put testing "should stay after backup is fetched"

mkdir -p $WALG_FILE_PREFIX

wal-g backup-push

expected_output=$(etcdctl get "" --prefix=true)

pkill etcd
wal-g backup-fetch LATEST

etcd --data-dir $ETCD_RESTORE_DATA_DIR &

actual_output=$(etcdctl get "" --prefix=true)

if [ "$actual_output" != "$expected_output" ]; then
  echo "Error: actual output doesn't match expected output"
  echo "Expected output: $expected_output"
  echo "Actual output: $actual_output"
  exit 1
fi

pkill etcd