#!/bin/sh
rm -rf vendor/github.com/google/brotli/*
mv .brotli.tmp/* vendor/github.com/google/brotli/
rm -rf .brotli.tmp
docker rm wal-g_s3_1 wal-g_pg_1
