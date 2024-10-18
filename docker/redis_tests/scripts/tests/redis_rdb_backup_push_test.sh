#!/bin/sh
set -e -x
. /tmp/lib.sh

sleep $REDIS_TIMEOUT
redis-server --save "900 0" --appendonly "no" --dir "/var/lib/redis" &
sleep $REDIS_TIMEOUT

export WALG_STREAM_CREATE_COMMAND="redis_cli.sh -a {password} --user default --rdb /dev/stdout"
wal-g backup-push # Send stream of dump to wal-g
wal-g rdb-backup-push # Send stream of dump to wal-g
wal-g backup-delete LATEST
wal-g backup-list

test_cleanup; echo "Redis (rdb-)backup-push test was successful"
