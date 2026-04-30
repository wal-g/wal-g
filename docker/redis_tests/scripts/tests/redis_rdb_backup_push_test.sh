#!/bin/sh
set -e -x
. /tmp/lib.sh

export WALG_STREAM_CREATE_COMMAND="redis_cli.sh -a {password} --user default --rdb /dev/stdout"

sleep $REDIS_TIMEOUT
redis-server --save "900 0" --appendonly "no" --dir "/var/lib/redis" &
sleep $REDIS_TIMEOUT

wal-g backup-push # Send stream of dump to wal-g
wal-g rdb-backup-push # Send stream of dump to wal-g
wal-g backup-delete --confirm LATEST
wal-g backup-list

test_cleanup; echo "Redis (rdb-)backup-push test was successful"

sleep $REDIS_TIMEOUT
redis-server --save "" --appendonly "yes" --dir "/var/lib/redis" --cluster-enabled "yes" \
  --cluster-config-file "/tmp/nodes.conf" &
sleep $REDIS_TIMEOUT

wal-g rdb-backup-push
wal-g backup-list
wal-g backup-delete --confirm LATEST

redis-cli cluster addslots 1
wal-g rdb-backup-push --walg-redis-fqdn-to-id-map "{\"$(hostname)\": \"id1\"}" \
  --walg-redis-cluster-conf-path /tmp/nodes.conf --sharded
wal-g backup-list
ensure "{\"id1\":[[\"1\",\"1\"]]}" $(wal-g backup-info --tag Slots LATEST --walg-download-file-retries 0)
wal-g backup-delete --confirm LATEST

test_cleanup; echo "Redis rdb-backup-push sharded test was successful"
