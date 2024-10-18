export REDIS_TIMEOUT=5
export FETCH_TIMEOUT=10
export WALG_FILE_PREFIX='/tmp/wal-g-test-data'
export WALE_S3_PREFIX=s3://redisbucket

test_cleanup() {
    redis-cli flushall
    redis-cli shutdown
    rm -rf /var/lib/redis/*
    rm -rf $WALG_FILE_PREFIX 
}

ensure() {
    _expected_output=$1
    if [ $# -eq 2 ]; then
        _actual_output=$2
    else
        _actual_output=$(redis-cli get key)
    fi

    if [ "$_actual_output" != "$_expected_output" ]; then
        echo "Error: actual output doesn't match expected output"
        echo "Expected output: $_expected_output"
        echo "Actual output: $_actual_output"
        exit 1
    fi
}
