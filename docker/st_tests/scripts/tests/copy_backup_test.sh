#!/bin/bash
set -euo pipefail
set -x

SOURCE_CONFIG=/tmp/copy-source.yaml
SERVER_TARGET_CONFIG=/tmp/copy-server-target.yaml
STREAM_SOURCE_CONFIG=/tmp/copy-stream-source.yaml
STREAM_TARGET_CONFIG=/tmp/copy-stream-target.yaml
BACKUP_NAME=base_copy_transport_test
BACKUP_PATH=basebackups_005/$BACKUP_NAME
SENTINEL_PATH=basebackups_005/${BACKUP_NAME}_backup_stop_sentinel.json

write_config() {
    local config_path=$1
    local bucket=$2
    local compression=$3
    cat >"$config_path" <<EOH
WALG_COMPRESSION_METHOD: $compression
AWS_ENDPOINT: "$AWS_ENDPOINT"
WALE_S3_PREFIX: "s3://$bucket"
AWS_S3_FORCE_PATH_STYLE: $AWS_S3_FORCE_PATH_STYLE
AWS_ACCESS_KEY_ID: "$AWS_ACCESS_KEY_ID"
AWS_SECRET_ACCESS_KEY: "$AWS_SECRET_ACCESS_KEY"
WALG_UPLOAD_CONCURRENCY: 3
S3_MAX_PART_SIZE: 5242880
EOH
}

write_config "$SOURCE_CONFIG" copytestsource none
write_config "$SERVER_TARGET_CONFIG" copytestserverdestination none
write_config "$STREAM_SOURCE_CONFIG" copytestsource lz4
write_config "$STREAM_TARGET_CONFIG" copyteststreamdestination lz4

head -c 1M </dev/urandom >/tmp/copy-small.bin
head -c 21M </dev/urandom >/tmp/copy-large.bin
printf '{}' >/tmp/copy-sentinel.json

wal-g --config "$SOURCE_CONFIG" st put /tmp/copy-small.bin "$BACKUP_PATH/small.bin" --no-compress
wal-g --config "$SOURCE_CONFIG" st put /tmp/copy-large.bin "$BACKUP_PATH/large.bin" --no-compress
wal-g --config "$SOURCE_CONFIG" st put /tmp/copy-sentinel.json "$SENTINEL_PATH" --no-compress

wal-g-etcd copy --from "$SOURCE_CONFIG" --to "$SERVER_TARGET_CONFIG" --backup-name "$BACKUP_NAME" \
    >/tmp/server-copy.log 2>&1
cat /tmp/server-copy.log
grep -F 'using S3 server-side copy (single)' /tmp/server-copy.log
grep -F 'using S3 server-side copy (multipart)' /tmp/server-copy.log

wal-g --config "$SERVER_TARGET_CONFIG" st get "$BACKUP_PATH/small.bin" /tmp/server-small.bin --no-decompress
wal-g --config "$SERVER_TARGET_CONFIG" st get "$BACKUP_PATH/large.bin" /tmp/server-large.bin --no-decompress
cmp /tmp/copy-small.bin /tmp/server-small.bin
cmp /tmp/copy-large.bin /tmp/server-large.bin

wal-g-etcd copy --from "$STREAM_SOURCE_CONFIG" --to "$STREAM_TARGET_CONFIG" --backup-name "$BACKUP_NAME" \
    >/tmp/stream-copy.log 2>&1
cat /tmp/stream-copy.log
grep -F 'Streamed ' /tmp/stream-copy.log
if grep -F 'using S3 server-side copy' /tmp/stream-copy.log; then
    echo 'ineligible copy unexpectedly used S3 server-side copy'
    exit 1
fi

wal-g --config "$STREAM_TARGET_CONFIG" st get "$BACKUP_PATH/small.bin" /tmp/stream-small.bin --no-decompress
wal-g --config "$STREAM_TARGET_CONFIG" st get "$BACKUP_PATH/large.bin" /tmp/stream-large.bin --no-decompress
cmp /tmp/copy-small.bin /tmp/stream-small.bin
cmp /tmp/copy-large.bin /tmp/stream-large.bin
