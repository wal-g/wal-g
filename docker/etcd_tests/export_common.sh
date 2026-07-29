#!/usr/bin/env bash

export WALG_STREAM_CREATE_COMMAND='TMP_DIR=$(mktemp) && etcdctl snapshot save $TMP_DIR > /dev/null && cat < $TMP_DIR'
export WALG_STREAM_RESTORE_COMMAND='TMP_DIR=$(mktemp) && cat > $TMP_DIR && etcdctl snapshot restore $TMP_DIR --data-dir $ETCD_RESTORE_DATA_DIR'
export WALG_FILE_PREFIX='/tmp/wal-g'
export WALG_ETCD_DATA_DIR='/tmp/etcd/cluster'
export ETCD_RESTORE_DATA_DIR='/tmp/etcd/restore_cluster'
export ETCD_LOG_LEVEL='info'
export ETCDCTL_API=3

etcd_kill_and_clean_data() {
    etcd --data-dir $WALG_ETCD_DATA_DIR &
    etcdctl del "" --from-key=true
    pkill etcd

    rm -rf "${WALG_FILE_PREFIX}"
    rm -rf "${ETCD_RESTORE_DATA_DIR}"
    rm -rf /root/.walg_mysql_binlogs_cache
}

fill_wal_data() {
    dd if=/dev/urandom bs=1024 count=50 | base64 > /tmp/large_file.txt
    FILE_CONTENT="$(cat /tmp/large_file.txt | tr '\n' ' ' | tr -d ' ')"
    i=0
    while [ $(find $WALG_ETCD_DATA_DIR/member/wal -type f -name \*.wal | wc -l) -lt $1 ]
    do
        KEY="key$i"
        STATUS=$(etcdctl put $KEY $FILE_CONTENT)
        i=$((i+1))
    done
}