#!/bin/bash
set -e -x

export WALG_STREAM_CREATE_COMMAND='TMP_DIR=$(mktemp -d) && chmod 777 $TMP_DIR && fdbbackup start -d file://$TMP_DIR -w 1>&2 && tar -c -C $TMP_DIR .'
export WALG_STREAM_RESTORE_COMMAND='TMP_DIR=$(mktemp -d) && chmod 777 $TMP_DIR && tar -xf - -C $TMP_DIR && BACKUP_DIR=$(find $TMP_DIR -mindepth 1 -print -quit) && fdbrestore start -r file://$BACKUP_DIR -w --dest_cluster_file "/var/fdb/fdb.cluster"  1>&2'
export WALG_FILE_PREFIX='/tmp/wal-g'

fdbcli -C /var/fdb/fdb.cluster --exec 'configure new single memory; writemode on; set test_key test_value'

mkdir $WALG_FILE_PREFIX

wal-g backup-push

fdbcli -C /var/fdb/fdb.cluster --exec 'writemode on; clearrange "" \xFF'

wal-g backup-fetch LATEST

actual_output=$(fdbcli -C /var/fdb/fdb.cluster --exec 'get test_key')

if [ "$actual_output" != "\`test_key' is \`test_value'" ]; then
  echo "Unexpected output: $actual_output"
  exit 1
fi