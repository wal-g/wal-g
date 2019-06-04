#!/bin/sh
set -e -x

# export WALE_S3_PREFIX=s3://mongobucket
export WALE_FILE_PREFIX=/tmp/wal-g-mongo-test-data
export WALG_MONGO_OPLOG_DST=./tmp/fetched_oplogs

mkdir $WALE_FILE_PREFIX
mkdir $WALG_MONGO_OPLOG_DST

add_test_data() {
    mongo --eval "for (var i = 0; i < 10; i++) { db.getSiblingDB('test').testData.save({x: i}) }"
}

service mongodb start

sleep 1

add_test_data

mongodump --archive --oplog | wal-g stream-push

add_test_data
mongoexport -d test -c testData | sort  > /tmp/export1.json

mongodump -d local -c oplog.\$main --out /tmp/oplog_dump
cat /tmp/oplog_dump/local/oplog.\$main.bson | wal-g oplog-push

sleep 1
export WALG_MONGO_OPLOG_END_TS=`date --rfc-3339=ns | sed 's/ /T/'`

pkill -9 mongod
rm -rf /var/lib/mongodb/*
service mongodb start

wal-g stream-fetch LATEST | mongorestore --archive --oplogReplay

ls $WALG_MONGO_OPLOG_DST
mv $WALG_MONGO_OPLOG_DST/`ls $WALG_MONGO_OPLOG_DST | head -n 1` $WALG_MONGO_OPLOG_DST/oplog.bson
mongorestore --oplogReplay $WALG_MONGO_OPLOG_DST

mongoexport -d test -c testData | sort  > /tmp/export2.json

pkill -9 mongod

diff /tmp/export1.json /tmp/export2.json

rm -rf $WALE_FILE_PREFIX
rm -rf $WALG_MONGO_OPLOG_DST
rm /tmp/export?.json
