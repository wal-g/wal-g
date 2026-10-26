#!/bin/sh
set -e -x

rclone version

# The Rclone remote is defined only through RCLONE_CONFIG_<REMOTE>_* environment variables:
# WAL-G has to hand its own environment over to the rclone processes it starts.
export RCLONE_CONFIG_S3TEST_TYPE=s3
export RCLONE_CONFIG_S3TEST_PROVIDER=Other
export RCLONE_CONFIG_S3TEST_ENDPOINT="$AWS_ENDPOINT"
export RCLONE_CONFIG_S3TEST_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID"
export RCLONE_CONFIG_S3TEST_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY"
export RCLONE_CONFIG_S3TEST_FORCE_PATH_STYLE=true

rclone mkdir s3test:rclonebucket

export RCLONE_REMOTE=s3test
# Small chunks force multipart uploads for the streamed uploads.
export RCLONE_S3_CHUNK_SIZE=5242880
export RCLONE_UPLOAD_CONCURRENCY=2
# The binary is built without optional codecs.
export WALG_COMPRESSION_METHOD=lz4

head -c 30M </dev/urandom >testfile

check_storage() {
    export WALG_RCLONE_PREFIX="rclone://rclonebucket/$1"

    # Empty list on empty storage
    test "1" -eq "$(wal-g st ls | wc -l)"

    # Upload a file, nested into folders that do not exist yet
    wal-g st put testfile testfolder/testfile
    wal-g st put testfile a/b/nested

    # Should not upload the duplicate file by default
    wal-g st put testfile testfolder/testfile && EXIT_STATUS=$? || EXIT_STATUS=$?
    if [ "$EXIT_STATUS" -eq 0 ] ; then
        echo "Error: Duplicate object was uploaded without the -f flag"
        exit 1
    fi

    # Should upload the duplicate file if -f flag is present
    wal-g st put testfile testfolder/testfile -f

    # The uploaded files are listed
    wal-g st ls testfolder | grep "testfile.lz4"
    wal-g st ls a/b | grep "nested.lz4"
    wal-g st stat testfolder/testfile.lz4

    # Downloaded file should be identical to the original one
    wal-g st get testfolder/testfile.lz4 fetched_testfile
    diff testfile fetched_testfile
    rm fetched_testfile

    # Uncompressed upload and download
    wal-g st put testfile testfolder/plain --no-compress
    wal-g st get testfolder/plain plain_fetched --no-decompress
    diff testfile plain_fetched
    rm plain_fetched

    # Reading the object back as a stream
    wal-g st cat testfolder/plain >/dev/null

    # Should be able to delete the uploaded file, and only it
    wal-g st rm testfolder/testfile.lz4
    if wal-g st stat testfolder/testfile.lz4; then
        echo "Error: Removed object is still present"
        exit 1
    fi
    wal-g st stat testfolder/plain
}

check_storage walg-env-only

# The same must keep working when a (here empty) Rclone config file is set: the file must not hide
# the remote that is defined through the environment.
: >/tmp/empty-rclone.conf
export RCLONE_CONFIG_PATH=/tmp/empty-rclone.conf
check_storage walg-with-config-path
