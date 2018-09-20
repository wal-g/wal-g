#!/bin/sh
mv vendor/github.com/google/brotli/go/cbrotli/cgo.go.tmp vendor/github.com/google/brotli/go/cbrotli/cgo.go
rm -rf vendor/github.com/google/brotli/dist/
docker rm wal-g_s3_1 wal-g_pg_1
