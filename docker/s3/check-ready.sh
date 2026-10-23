#!/bin/sh
set -eu

# An open S3 port is not sufficient: mini creates S3_BUCKET only after all
# services have started. Check every configured bucket's filer directory too.
wget -q --spider -T 2 http://127.0.0.1:9333/cluster/status
wget -S --spider -T 2 http://127.0.0.1:9000 2>&1 | grep -q 'HTTP/1.1'

if [ -z "${S3_BUCKET:-}" ]; then
    echo 'S3_BUCKET must name the integration-test buckets' >&2
    exit 1
fi
IFS=,
set -f
for bucket in $S3_BUCKET; do
    case "$bucket" in
        ''|*[!a-z0-9.-]*) echo "Invalid test bucket: $bucket" >&2; exit 1 ;;
    esac
    if ! wget -q --spider -T 2 "http://127.0.0.1:8888/buckets/$bucket/"; then
        echo "Test bucket is not ready: $bucket" >&2
        exit 1
    fi
done
