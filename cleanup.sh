#!/bin/sh

rm -rf tmp/
rm -rf vendor/github.com/google/brotli/*

walg_images=$(docker ps --all --format '{{.Names}}' | grep "wal-g/*")

if [ -n "${walg_images}" ]; then
    docker rm -f "${walg_images}"
fi

bad_images=$(docker images --filter "dangling=true" --quiet --no-trunc)

if [ -n "${bad_images}" ]; then
    docker rmi "${bad_images}"
fi
