#!/bin/sh

docker rm -f $(docker ps --all --format '{{.Names}}' | grep wal-g_*)
