#!/bin/sh
set -e -x

export WALE_S3_PREFIX=s3://redisbucket
export WALG_STREAM_CREATE_COMMAND="redis-cli -a {password} --rdb /dev/stdout"

sleep 10

service redis-server start &

sleep 10

wal-g backup-push --permanent

backup_to_delete_name=`wal-g backup-list | tail -n 1 | cut -f 1 -d " "`

lines_before_delete=`wal-g backup-list | wc -l`
wal-g backup-list > /tmp/list_before_delete

wal-g delete --target $backup_to_delete_name --confirm

lines_after_delete=`wal-g backup-list | wc -l`
wal-g backup-list > /tmp/list_after_delete

if [ $lines_before_delete -ne $lines_after_delete ]
then
    echo $lines_before_delete > /tmp/before_delete
    echo $lines_after_delete > /tmp/after_delete
    echo "Permanent backup does not exist after deletion"
    diff /tmp/before_delete /tmp/after_delete
fi

redis-cli shutdown
