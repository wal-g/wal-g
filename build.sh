#!/bin/sh
set -e

readonly CWD=$PWD
cd vendor/github.com/google/brotli/go/cbrotli
readonly LIB_DIR=../../dist  # dist will contain binaries and it is in the google/brotli/.gitignore
# patch cgo.go to force usage of static libraries for linking
sed -e "s|#cgo LDFLAGS: -lbrotlicommon|#cgo CFLAGS: -I../../c/include|" \
    -e "s|\(#cgo LDFLAGS:\) \(-lbrotli.*\)|\1 -L$LIB_DIR \2-static -lbrotlicommon-static|" \
    -e "/ -lm$/ n; /brotlienc/ s|$| -lm|" -i cgo.go

mkdir -p $LIB_DIR
cd $LIB_DIR
../configure-cmake --disable-debug
make
cd $CWD

make
cp ./cmd/wal-g/wal-g ./docker/pg
docker-compose build
docker-compose up --exit-code-from pg
