#!/bin/bash

rm -rf tmp/
rm -rf vendor/github.com/google/brotli/*

walg_images=$(docker ps --all --format '{{.Names}}' | grep wal-g_*)

if [[ ${walg_images} ]]; then
    docker rm -f ${walg_images}
fi

bad_images=$(docker images --filter "dangling=true" --quiet --no-trunc)

if [[ ${bad_images} ]]; then
    docker rmi ${bad_images}
fi
