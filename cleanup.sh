#!/bin/sh

if $(docker ps --all --format '{{.Names}}' | grep wal-g_*) > /dev/null; then
    docker rm -f $(docker ps --all --format '{{.Names}}' | grep wal-g_*)
fi
