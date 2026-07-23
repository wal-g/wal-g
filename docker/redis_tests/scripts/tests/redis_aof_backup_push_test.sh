#!/bin/sh
set -e -x
. /tmp/lib.sh

sleep $REDIS_TIMEOUT
redis-server --save "" --appendonly "yes" --dir "/var/lib/redis" &
sleep $REDIS_TIMEOUT

wal-g backup-push --type aof # Send stream of aof-files to wal-g
wal-g backup-list
wal-g backup-delete --confirm LATEST

test_cleanup; echo "Redis backup-push --type aof test was successful"

sleep $REDIS_TIMEOUT
redis-server --save "" --appendonly "yes" --dir "/var/lib/redis" --cluster-enabled "yes" \
  --cluster-config-file "/tmp/nodes.conf" &
sleep $REDIS_TIMEOUT

wal-g backup-push --type aof
wal-g backup-list
wal-g backup-delete --confirm LATEST

redis-cli cluster addslots 1
wal-g backup-push --type aof --walg-redis-fqdn-to-id-map "{\"$(hostname)\": \"id1\"}" \
  --walg-redis-cluster-conf-path /tmp/nodes.conf --sharded
wal-g backup-list
ensure "{\"id1\":[[\"1\",\"1\"]]}" $(wal-g backup-info --tag Slots LATEST --walg-download-file-retries 0)
wal-g backup-delete --confirm LATEST

test_cleanup; echo "Redis backup-push --type aof sharded test was successful"
