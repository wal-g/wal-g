#!/bin/sh
set -e
cp -rf .brotli/dist vendor/github.com/google/brotli/
cp vendor/github.com/google/brotli/go/cbrotli/cgo.go vendor/github.com/google/brotli/go/cbrotli/cgo.go.tmp

readonly CWD=$PWD
cd vendor/github.com/google/brotli/go/cbrotli
readonly LIB_DIR=../../dist
# patch cgo.go to force usage of static libraries for linking
sed -e "s|#cgo LDFLAGS: -lbrotlicommon|#cgo CFLAGS: -I../../c/include|" \
    -e "s|\(#cgo LDFLAGS:\) \(-lbrotli.*\)|\1 -L$LIB_DIR \2-static -lbrotlicommon-static|" \
    -e "/ -lm$/ n; /brotlienc/ s|$| -lm|" -i cgo.go
cd $CWD

make
cp ./cmd/wal-g/wal-g ./docker/pg
docker-compose build
docker-compose up --exit-code-from pg
mv vendor/github.com/google/brotli/go/cbrotli/cgo.go.tmp vendor/github.com/google/brotli/go/cbrotli/cgo.go
rm -rf vendor/github.com/google/brotli/dist/
