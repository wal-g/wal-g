#!/bin/sh
set -e -x

export WALE_S3_PREFIX=s3://mongostreampushbucket
export WALG_MONGO_OPLOG_DST=./tmp/fetched_oplogs

mkdir $WALG_MONGO_OPLOG_DST

add_test_data() {
    mongo --eval "for (var i = 0; i < 10; i++) { db.getSiblingDB('test').testData.save({x: i}) }"
}

service mongodb start

for i in $(seq 1 5);
do
    sleep 1
    add_test_data
    mongodump --archive --oplog | wal-g stream-push

    if [ $i -eq 3 ];
    then
        mongoexport -d test -c testData | sort  > /tmp/export1.json
    fi
done

pkill -9 mongod
rm -rf /var/lib/mongodb/*
service mongodb start

wal-g backup-list

backup_name=`wal-g backup-list | tail -n 3 | head -n 1 | cut -f 1 -d " "`

wal-g stream-fetch $backup_name | mongorestore --archive --oplogReplay

mongoexport -d test -c testData | sort  > /tmp/export2.json

pkill -9 mongod

diff /tmp/export1.json /tmp/export2.json

rm -rf $WALG_MONGO_OPLOG_DST
rm /tmp/export?.json
