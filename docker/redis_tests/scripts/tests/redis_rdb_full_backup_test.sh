#!/bin/sh
set -e -x
. /tmp/lib.sh

export WALG_STREAM_CREATE_COMMAND="redis_cli.sh --rdb /dev/stdout"
export WALG_STREAM_RESTORE_COMMAND="cat > /var/lib/redis/dump.rdb"
export WALG_COMPRESSION_METHOD=lz4

sleep $REDIS_TIMEOUT
redis-server --save "900 0" --appendonly "no" --dir "/var/lib/redis" &
sleep $REDIS_TIMEOUT

redis-cli set key test_value
expected_output=$(redis-cli get key)

mkdir $WALG_FILE_PREFIX
wal-g rdb-backup-push

ensure rdb $(wal-g backup-info --tag BackupType LATEST)

test_cleanup; sleep $REDIS_TIMEOUT

touch /var/lib/redis/fake.aof
touch /var/lib/redis/fake.rdb
mkdir /var/lib/redis/appendonlydir
touch /var/lib/redis/appendonlydir/fake.tmp
wal-g rdb-backup-fetch LATEST
ensure no $(test -e /var/lib/redis/fake.aof && echo "yes" || echo "no")
ensure no $(test -e /var/lib/redis/fake.rdb && echo "yes" || echo "no")
ensure no $(test -e /var/lib/redis/appendonlydir/fake.tmp && echo "yes" || echo "no")

redis-server --save "900 0" --appendonly "no" --dir "/var/lib/redis" &
sleep $REDIS_TIMEOUT

ensure $expected_output

test_cleanup; echo "Redis (rdb-)full-backup-push test was successful"

sleep $REDIS_TIMEOUT
redis-server --save "900 0" --appendonly "no" --dir "/var/lib/redis" &
sleep $REDIS_TIMEOUT

redis-cli set key test_value
expected_output=$(redis-cli get key)

mkdir $WALG_FILE_PREFIX
wal-g backup-push
wal-g backup-info LATEST

test_cleanup; sleep $REDIS_TIMEOUT

touch /var/lib/redis/fake.aof
touch /var/lib/redis/fake.rdb
wal-g backup-fetch LATEST
ensure no $(test -e /var/lib/redis/fake.aof && echo "yes" || echo "no")
ensure no $(test -e /var/lib/redis/fake.rdb && echo "yes" || echo "no")

redis-server --save "900 0" --appendonly "no" --dir "/var/lib/redis" &
sleep $REDIS_TIMEOUT

ensure $expected_output

test_cleanup; echo "Redis (rdb-old)full-backup-push test was successful"
