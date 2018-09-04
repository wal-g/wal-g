#!/bin/sh
make
code = $?
if [ "$code" -ne 0  ]
then
	exit $code
fi
cp ./cmd/wal-g/wal-g ./docker/pg
docker-compose build
docker-compose up --exit-code-from pg
exit $?
