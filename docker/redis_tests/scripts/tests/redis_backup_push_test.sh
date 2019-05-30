#!/bin/sh
set -e -x

export WALE_S3_PREFIX=s3://redisbucket

sleep 10 # Wait until port 6379 will be available

service redis-server start &

sleep 10 # Wait for full redis-server start

wal-g backup-push

wal-g backup-list

echo "Redis backup-push test was successful"
