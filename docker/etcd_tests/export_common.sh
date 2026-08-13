#!/usr/bin/env bash

export WALG_STREAM_CREATE_COMMAND='TMP_DIR=$(mktemp) && etcdctl snapshot save $TMP_DIR > /dev/null && cat < $TMP_DIR'
export WALG_STREAM_RESTORE_COMMAND='TMP_DIR=$(mktemp) && cat > $TMP_DIR && etcdctl snapshot restore $TMP_DIR --data-dir $ETCD_RESTORE_DATA_DIR'
export WALG_FILE_PREFIX='/tmp/wal-g'
export WALG_ETCD_DATA_DIR='/tmp/etcd/cluster'
export ETCD_RESTORE_DATA_DIR='/tmp/etcd/restore_cluster'
export ETCD_LOG_LEVEL='info'
export ETCDCTL_API=3
export ETCD_ENDPOINT='http://127.0.0.1:2379'

wait_for_etcd() {
    for _ in $(seq 1 60); do
        if etcdctl --endpoints "$ETCD_ENDPOINT" endpoint health >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done

    echo "etcd failed to become ready in time" >&2
    return 1
}

start_etcd() {
    local data_dir="${1:-$WALG_ETCD_DATA_DIR}"
    etcd \
        --data-dir "$data_dir" \
        --listen-client-urls "$ETCD_ENDPOINT" \
        --advertise-client-urls "$ETCD_ENDPOINT" \
        --listen-peer-urls http://127.0.0.1:2380 \
        --initial-advertise-peer-urls http://127.0.0.1:2380 \
        --initial-cluster default=http://127.0.0.1:2380 \
        >/tmp/etcd.log 2>&1 &

    wait_for_etcd
}

kill_etcd() {
    pkill -x etcd || true
    for _ in $(seq 1 50); do
        if ! pgrep -x etcd >/dev/null; then
            break
        fi
        sleep 0.2
    done

    if pgrep -x etcd >/dev/null; then
        pkill -9 -x etcd || true
    fi
}

etcd_kill_and_clean_data() {
    start_etcd "$WALG_ETCD_DATA_DIR"
    etcdctl --endpoints "$ETCD_ENDPOINT" del "" --from-key=true
    kill_etcd

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