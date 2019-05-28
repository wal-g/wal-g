#!/bin/sh
set -e -x

export WALE_S3_PREFIX=s3://redisbucket

service redis-server start &

sleep 1

wal-g backup-push

wal-g backup-list

echo "Redis backup-push test was successful"
