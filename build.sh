#!/bin/sh
set -e -x

if ! which dep > /dev/null; then
    go get -u github.com/golang/dep/cmd/dep
fi

make

docker-compose build
docker-compose up --exit-code-from pg pg
