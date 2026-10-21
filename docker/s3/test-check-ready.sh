#!/bin/sh
set -eu
check_script=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)/check-ready.sh

probe() {
    S3_BUCKET=$1 MISSING_URL=$2 sh -c '
        wget() {
            for arg do url=$arg; done
            [ "$url" != "$MISSING_URL" ] || return 1
            case "$url" in
                http://127.0.0.1:9000) echo "HTTP/1.1 403 Forbidden" >&2 ;;
            esac
        }
        . "$1"
    ' -- "$check_script"
}

probe first,second ''
for missing in \
    http://127.0.0.1:9333/cluster/status \
    http://127.0.0.1:9000 \
    http://127.0.0.1:8888/buckets/first/ \
    http://127.0.0.1:8888/buckets/second/; do
    if probe first,second "$missing"; then
        echo "Healthcheck accepted unavailable endpoint: $missing" >&2
        exit 1
    fi
done
for buckets in '' '../invalid' 'bad bucket'; do
    if probe "$buckets" ''; then
        echo "Healthcheck accepted invalid bucket list: $buckets" >&2
        exit 1
    fi
done
echo 'S3 readiness checks passed'
