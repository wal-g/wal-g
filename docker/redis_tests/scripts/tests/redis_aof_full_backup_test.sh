#!/bin/sh
set -e -x
. /tmp/lib.sh

export WALG_COMPRESSION_METHOD=lz4

sleep $REDIS_TIMEOUT
redis-server --save "" --appendonly "yes" --dir "/var/lib/redis" &
sleep $REDIS_TIMEOUT

redis-cli set key test
sleep $REDIS_TIMEOUT # Wait for aof files

expected_output=$(redis-cli get key)

mkdir $WALG_FILE_PREFIX
wal-g aof-backup-push

test_cleanup; sleep $REDIS_TIMEOUT

wal-g aof-backup-fetch LATEST 7.2.5
redis-server --save "" --appendonly "yes" --dir "/var/lib/redis" &
sleep $REDIS_TIMEOUT

ensure $expected_output

test_cleanup; echo "Redis aof-full-backup-push test was successful"
