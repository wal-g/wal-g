#!/bin/sh
set -e -x

add_test_data() {
    mongo --eval "for (var i = 0; i < 10; i++) { db.getSiblingDB('test').testData.save({x: i}) }"
}

test_delete_command() {
    DELETE_COMMAND=$1
    OPLOG_DUMP_DIR=/tmp/oplog_dump

    mkdir -p "$WALG_MONGO_OPLOG_DST"
    mkdir -p $OPLOG_DUMP_DIR
    export WALG_STREAM_CREATE_COMMAND="mongodump --archive --oplog"
    service mongodb start

    for i in $(seq 1 5);
    do
        sleep 1
        add_test_data

        wal-g backup-push

        if [ "$i" -eq 3 ];
        then
            mongoexport -d test -c testData | sort  > /tmp/export1.json
        fi
        sleep 1
        mongodump -d local -c oplog.\$main --out $OPLOG_DUMP_DIR
        cat $OPLOG_DUMP_DIR/local/oplog.\$main.bson | wal-g oplog-push
    done

    wal-g backup-list

    $DELETE_COMMAND

    wal-g backup-list

    pkill -9 mongod

    rm -rf /var/lib/mongodb/*
    service mongodb start

    first_backup_name=$(wal-g backup-list | head -n 2 | tail -n 1 | cut -f 1 -d " ")

    wal-g backup-fetch "${MONGODBDATA}" "$first_backup_name" | mongorestore --archive --oplogReplay
    wal-g oplog-fetch --since "$first_backup_name"

    mongoexport -d test -c testData | sort  > /tmp/export2.json

    pkill -9 mongod

    diff /tmp/export1.json /tmp/export2.json

    oplogCount=$(ls "$WALG_MONGO_OPLOG_DST" | wc -l)
    if [ "$oplogCount" -ne 3 ]
    then
        echo "Expected oplog count is 3. Actual: $oplogCount"
        exit 1
    fi

    rm -rf $OPLOG_DUMP_DIR
    rm -rf "$WALG_MONGO_OPLOG_DST"
    rm /tmp/export?.json
}

export WALG_MONGO_OPLOG_DST=/tmp/fetched_oplogs

delete_before_name() {
    backup_name=$(wal-g backup-list | tail -n 3 | head -n 1 | cut -f 1 -d " ")

    wal-g delete before "$backup_name" --confirm
}

export WALE_S3_PREFIX=s3://mongodeletebeforebucket
test_delete_command delete_before_name

delete_retain() {
    wal-g delete retain 3 --confirm
}

export WALE_S3_PREFIX=s3://mongodeleteretainbucket
test_delete_command delete_retain
