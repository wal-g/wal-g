#!/bin/sh
set -e -x

export WALE_S3_PREFIX=s3://storagetoolsbucket

# Empty list on empty storage
test "1" -eq "$(wal-g st ls | wc -l)"

# Generate and upload some file to storage
head -c 100M </dev/urandom >testfile
wal-g st put testfile testfolder/testfile

# Should not upload the duplicate file by default
wal-g st put testfile testfolder/testfile && EXIT_STATUS=$? || EXIT_STATUS=$?

if [ "$EXIT_STATUS" -eq 0 ] ; then
    echo "Error: Duplicate object was uploaded without the -f flag"
    exit 1
fi

# Should upload the duplicate file if -f flag is present
wal-g st put testfile testfolder/testfile -f

wal-g st ls
# WAL-G should show the uploaded file in the wal-g st ls output
test "2" -eq "$(wal-g st ls | wc -l)"

# WAL-G should be able to download the uploaded file
wal-g st get -m decompress testfolder/testfile.br fetched_testfile

# Downloaded file should be identical to the original one
diff testfile fetched_testfile

# WAL-G should be able to delete the uploaded file
wal-g st rm testfolder/testfile.br

# Should get empty storage after file removal
test "1" -eq "$(wal-g st ls | wc -l)"