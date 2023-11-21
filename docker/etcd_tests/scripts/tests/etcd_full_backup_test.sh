#!/bin/bash
set -e -x

export WALG_STREAM_CREATE_COMMAND='TMP_DIR=$(mktemp) && etcdctl --endpoints=localhost:2379 snapshot save $TMP_DIR > /dev/null && cat < $TMP_DIR'
export WALG_STREAM_RESTORE_COMMAND='TMP_DIR=$(mktemp) && cat > $TMP_DIR && etcdctl snapshot restore $TMP_DIR --data-dir /tmp/etcd/cluster'
export WALG_FILE_PREFIX='/tmp/wal-g'

brew services start etcd

etcdctl put greeting "Hello, etcd"

mkdir $WALG_FILE_PREFIX

wal-g backup-push

expected_output=$(etcdctl get "" --prefix=true)

wal-g backup-fetch LATEST

actual_output=$(etcdctl get "" --prefix=true)

if [ "$actual_output" != "$expected_output" ]; then
  echo "Error: actual output doesn't match expected output"
  echo "Expected output: $expected_output"
  echo "Actual output: $actual_output"
  exit 1
fi

brew services stop etcd