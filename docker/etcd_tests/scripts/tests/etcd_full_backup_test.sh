#!/bin/bash
set -e -x

. /usr/local/export_common.sh

start_etcd "$WALG_ETCD_DATA_DIR"

etcdctl --endpoints "$ETCD_ENDPOINT" put testing "should stay after backup is fetched"

mkdir -p $WALG_FILE_PREFIX

wal-g backup-push

expected_output=$(etcdctl get "" --prefix=true)

kill_etcd
wal-g backup-fetch LATEST

start_etcd "$ETCD_RESTORE_DATA_DIR"

actual_output=$(etcdctl --endpoints "$ETCD_ENDPOINT" get "" --prefix=true)

if [ "$actual_output" != "$expected_output" ]; then
  echo "Error: actual output doesn't match expected output"
  echo "Expected output: $expected_output"
  echo "Actual output: $actual_output"
  exit 1
fi

kill_etcd