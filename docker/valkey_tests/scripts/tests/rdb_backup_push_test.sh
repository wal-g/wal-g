#!/bin/sh
set -e -x
. /tmp/lib.sh

export WALG_STREAM_CREATE_COMMAND="valkey-cli -a {password} --user default --rdb /dev/stdout"

sleep $VALKEY_TIMEOUT
valkey-server --save "900 0" --appendonly "no" --dir "/var/lib/valkey" &
sleep $VALKEY_TIMEOUT

wal-g backup-push # Send stream of dump to wal-g
wal-g rdb-backup-push # Send stream of dump to wal-g
wal-g backup-delete --confirm LATEST
wal-g backup-list

test_cleanup; echo "Valkey (rdb-)backup-push test was successful"

sleep $VALKEY_TIMEOUT
valkey-server --save "" --appendonly "yes" --dir "/var/lib/valkey" --cluster-enabled "yes" \
  --cluster-config-file "/tmp/nodes.conf" &
sleep $VALKEY_TIMEOUT

wal-g rdb-backup-push
wal-g backup-list
wal-g backup-delete --confirm LATEST

valkey-cli cluster addslots 1
wal-g rdb-backup-push --walg-redis-fqdn-to-id-map "{\"$(hostname)\": \"id1\"}" \
  --walg-redis-cluster-conf-path /tmp/nodes.conf --sharded
wal-g backup-list
ensure "{\"id1\":[[\"1\",\"1\"]]}" $(wal-g backup-info --tag Slots LATEST --walg-download-file-retries 0)
wal-g backup-delete --confirm LATEST

test_cleanup; echo "Valkey rdb-backup-push sharded test was successful"
