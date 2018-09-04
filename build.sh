#!/bin/sh
set -e
make
cp ./cmd/wal-g/wal-g ./docker/pg
docker-compose build
docker-compose up --exit-code-from pg
