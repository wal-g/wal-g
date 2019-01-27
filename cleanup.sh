#!/bin/sh

rm -rf tmp/

if docker ps --all --format '{{.Names}}' | grep wal-g_* > /dev/null; then
    docker rm -f $(docker ps --all --format '{{.Names}}' | grep wal-g_*)
fi

docker rmi $(docker images --filter "dangling=true" --quiet --no-trunc)
