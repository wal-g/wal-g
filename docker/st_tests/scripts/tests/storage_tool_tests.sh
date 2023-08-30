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
wal-g st get testfolder/testfile.br fetched_testfile

# Downloaded file should be identical to the original one
diff testfile fetched_testfile
rm fetched_testfile

# WAL-G should be able to download the uploaded file without decompression
wal-g st get testfolder/testfile.br uncompressed_testfile.br --no-decompress

brotli --decompress uncompressed_testfile.br
diff testfile uncompressed_testfile
rm uncompressed_testfile

# WAL-G should be able to delete the uploaded file
wal-g st rm testfolder/testfile.br

# Should get empty storage after file removal
test "1" -eq "$(wal-g st ls | wc -l)"

# Should upload the file uncompressed without error
wal-g st put testfile testfolder/testfile --no-compress 

# Should download the file uncompressed without error
wal-g st get testfolder/testfile uncompressed_file --no-decompress

diff testfile uncompressed_file

cat > conf1.yaml <<EOH
WALG_COMPRESSION_METHOD: brotli
WALG_DELTA_MAX_STEPS: 6
WALE_GPG_KEY_ID: "5697E1083B8509B8"
WALG_DISK_RATE_LIMIT: 67108864
WALG_NETWORK_RATE_LIMIT: 67108864
WALG_DOWNLOAD_CONCURRENCY: 1
WALG_UPLOAD_CONCURRENCY: $WALG_UPLOAD_CONCURRENCY
WALG_UPLOAD_DISK_CONCURRENCY: 1
GOMAXPROCS: 1
AWS_ENDPOINT: "$AWS_ENDPOINT"
WALE_S3_PREFIX: "$WALE_S3_PREFIX"
AWS_S3_FORCE_PATH_STYLE: $AWS_S3_FORCE_PATH_STYLE
AWS_ACCESS_KEY_ID: "$AWS_ACCESS_KEY_ID"
AWS_SECRET_ACCESS_KEY: "$AWS_SECRET_ACCESS_KEY"
EOH

cat > conf2.yaml <<EOH
WALG_COMPRESSION_METHOD: brotli
WALG_DELTA_MAX_STEPS: 6
WALE_GPG_KEY_ID: "5697E1083B8509B8"
WALG_DISK_RATE_LIMIT: 67108864
WALG_NETWORK_RATE_LIMIT: 67108864
WALG_DOWNLOAD_CONCURRENCY: 1
WALG_UPLOAD_CONCURRENCY: $WALG_UPLOAD_CONCURRENCY
WALG_UPLOAD_DISK_CONCURRENCY: 1
GOMAXPROCS: 1
AWS_ENDPOINT: "$AWS_ENDPOINT"
WALE_S3_PREFIX: "s3://storagetoolsbucket_target"
AWS_S3_FORCE_PATH_STYLE: $AWS_S3_FORCE_PATH_STYLE
AWS_ACCESS_KEY_ID: "$AWS_ACCESS_KEY_ID"
AWS_SECRET_ACCESS_KEY: "$AWS_SECRET_ACCESS_KEY"
EOH

wal-g --config ./conf1.yaml st copy --from ./conf1.yaml --to ./conf2.yaml

# Should get empty storage after file copy
test "2" -eq "$(wal-g --config ./conf2.yaml st ls | wc -l)"
