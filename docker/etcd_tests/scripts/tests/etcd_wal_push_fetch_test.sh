#!/bin/sh
set -e -x

. /usr/local/export_common.sh

etcd --data-dir $WALG_ETCD_DATA_DIR &

mkdir -p $WALG_FILE_PREFIX

wal-g backup-push

# put keys with big data to create so 3 wal files are created
set +x -v
fill_wal_data 3
set -x

wal-g wal-push

# create dir where to store fetched wals
mkdir -p /tmp/fetch_wal
wal-g wal-fetch "/tmp/fetch_wal"

#last wal should not be fetched, only complete wals are pushed and fetched
expected_fetched_wals=$(find $WALG_ETCD_DATA_DIR/member/wal -type f -name \*.wal -exec basename \{} \; | sort -n -t - -k 1 | head -n 2)
for file in $expected_fetched_wals; do
  if ! test -f /tmp/fetch_wal/$file; then
    echo "Error: Not all wals were fetched"
    echo "Missing file: /tmp/fetch_wal/$file"
    exit 1
  fi
  if ! cmp -s /tmp/fetch_wal/$file $WALG_ETCD_DATA_DIR/member/wal/$file; then
    echo "Error: Something went wrong while restoring wals"
    exit 1
  fi
done

pkill etcd