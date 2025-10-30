#!/bin/sh
set -e -x
. /tmp/lib.sh

sleep $VALKEY_TIMEOUT
valkey-server --save "" --appendonly "yes" --dir "/var/lib/valkey" --cluster-enabled "yes" &
sleep $VALKEY_TIMEOUT
valkey-cli --cluster-replicas 0 --cluster-yes --cluster create 127.0.0.1:6379 127.0.0.1:6379 127.0.0.1:6379
sleep $VALKEY_TIMEOUT

valkey-cli set key test
sleep $VALKEY_TIMEOUT # Wait for aof files
expected_output=$(valkey-cli get key)

mkdir $WALG_FILE_PREFIX
wal-g aof-backup-push

ensure aof $(wal-g backup-info --tag BackupType LATEST)

test_cleanup; sleep $VALKEY_TIMEOUT

touch /var/lib/valkey/fake.aof
touch /var/lib/valkey/fake.rdb
mkdir /var/lib/valkey/appendonlydir
touch /var/lib/valkey/appendonlydir/fake.tmp
wal-g aof-backup-fetch LATEST 7.2.5
ensure no $(test -e /var/lib/valkey/fake.aof && echo "yes" || echo "no")
ensure no $(test -e /var/lib/valkey/fake.rdb && echo "yes" || echo "no")
ensure no $(test -e /var/lib/valkey/appendonlydir/fake.tmp && echo "yes" || echo "no")

valkey-server --save "" --appendonly "yes" --dir "/var/lib/valkey" &
sleep $VALKEY_TIMEOUT

ensure $expected_output

test_cleanup; echo "Valkey aof-sharded-full-backup-push test was successful"
