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
    expected_output=$1
    actual_output=$(redis-cli get key)
    if [ "$actual_output" != "$expected_output" ]; then
        echo "Error: actual output doesn't match expected output"
        echo "Expected output: $expected_output"
        echo "Actual output: $actual_output"
        exit 1
    fi
}
