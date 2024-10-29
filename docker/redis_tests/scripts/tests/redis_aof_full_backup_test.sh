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

ensure aof $(wal-g backup-info --tag BackupType LATEST)

test_cleanup; sleep $REDIS_TIMEOUT

touch /var/lib/redis/fake.aof
touch /var/lib/redis/fake.rdb
mkdir /var/lib/redis/appendonlydir
touch /var/lib/redis/appendonlydir/fake.tmp
wal-g aof-backup-fetch LATEST 7.2.5
ensure no $(test -e /var/lib/redis/fake.aof && echo "yes" || echo "no")
ensure no $(test -e /var/lib/redis/fake.rdb && echo "yes" || echo "no")
ensure no $(test -e /var/lib/redis/appendonlydir/fake.tmp && echo "yes" || echo "no")

redis-server --save "" --appendonly "yes" --dir "/var/lib/redis" &
sleep $REDIS_TIMEOUT

ensure $expected_output

test_cleanup; echo "Redis aof-full-backup-push test was successful"
