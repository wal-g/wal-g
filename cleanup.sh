#!/bin/bash

rm -rf tmp/
rm -rf vendor/github.com/google/brotli/*

walg_containers=$(docker ps --all --format '{{.Names}}' --filter 'name=wal-g_')

if [[ ${walg_containers} ]]; then
    docker rm -f ${walg_containers}
fi

walg_images=$(docker images --filter='reference=wal-g/*' --quiet)
if [[ ${walg_images} ]]; then
    docker rmi ${walg_images}
fi

walg_volumes=$(docker volume ls --quiet --filter=name=wal-g_)
if [[ ${walg_volumes} ]]; then
    docker volume rm ${walg_volumes}
fi

bad_images=$(docker images --filter "dangling=true" --quiet --no-trunc)

if [[ ${bad_images} ]]; then
    docker rmi ${bad_images}
fi
