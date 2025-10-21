#!/bin/sh
set -e -x
. /tmp/lib.sh

export WALG_STREAM_CREATE_COMMAND="valkey-cli --rdb /dev/stdout"
export WALG_STREAM_RESTORE_COMMAND="cat > /var/lib/valkey/dump.rdb"

sleep $VALKEY_TIMEOUT
valkey-server --save "900 0" --appendonly "no" --dir "/var/lib/valkey" &
sleep $VALKEY_TIMEOUT

valkey-cli set key test_value
expected_output=$(valkey-cli get key)

mkdir $WALG_FILE_PREFIX
wal-g rdb-backup-push

ensure rdb $(wal-g backup-info --tag BackupType LATEST)

test_cleanup; sleep $VALKEY_TIMEOUT

touch /var/lib/valkey/fake.aof
touch /var/lib/valkey/fake.rdb
mkdir /var/lib/valkey/appendonlydir
touch /var/lib/valkey/appendonlydir/fake.tmp
wal-g rdb-backup-fetch LATEST
ensure no $(test -e /var/lib/valkey/fake.aof && echo "yes" || echo "no")
ensure no $(test -e /var/lib/valkey/fake.rdb && echo "yes" || echo "no")
ensure no $(test -e /var/lib/valkey/appendonlydir/fake.tmp && echo "yes" || echo "no")

valkey-server --save "900 0" --appendonly "no" --dir "/var/lib/valkey" &
sleep $VALKEY_TIMEOUT

ensure $expected_output

test_cleanup; echo "Valkey (rdb-)full-backup-push test was successful"

sleep $VALKEY_TIMEOUT
valkey-server --save "900 0" --appendonly "no" --dir "/var/lib/valkey" &
sleep $VALKEY_TIMEOUT

valkey-cli set key test_value
expected_output=$(valkey-cli get key)

mkdir $WALG_FILE_PREFIX
wal-g backup-push
wal-g backup-info LATEST

test_cleanup; sleep $VALKEY_TIMEOUT

touch /var/lib/valkey/fake.aof
touch /var/lib/valkey/fake.rdb
wal-g backup-fetch LATEST
ensure no $(test -e /var/lib/valkey/fake.aof && echo "yes" || echo "no")
ensure no $(test -e /var/lib/valkey/fake.rdb && echo "yes" || echo "no")

valkey-server --save "900 0" --appendonly "no" --dir "/var/lib/valkey" &
sleep $VALKEY_TIMEOUT

ensure $expected_output

test_cleanup; echo "Valkey (rdb-old)full-backup-push test was successful"
