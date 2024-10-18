#!/bin/sh
set -e -x
. /tmp/lib.sh

sleep $REDIS_TIMEOUT
redis-server --save "" --appendonly "yes" --dir "/var/lib/redis" &
sleep $REDIS_TIMEOUT

wal-g aof-backup-push # Send stream of aof-files to wal-g
wal-g backup-list
wal-g backup-delete LATEST

test_cleanup; echo "Redis aof-backup-push test was successful"
