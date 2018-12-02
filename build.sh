#!/bin/sh
set -e -x

if ! which dep > /dev/null; then
    go get -u github.com/golang/dep/cmd/dep  # install dependencies management system
fi

make deps  # install dependencies

test -d tmp  || mkdir tmp

cp -rf vendor/github.com/google/brotli/* tmp/
cp -rf submodules/brotli/* vendor/github.com/google/brotli/

readonly CWD=$PWD

cd vendor/github.com/google/brotli/go/cbrotli

readonly LIB_DIR=../../dist  # dist will contain binaries and it is in the google/brotli/.gitignore

# patch cgo.go to force usage of static libraries for linking
sed -i -e "s|#cgo LDFLAGS: -lbrotlicommon|#cgo CFLAGS: -I../../c/include|" cgo.go
sed -i -e "s|\(#cgo LDFLAGS:\) \(-lbrotli.*\)|\1 -L$LIB_DIR \2-static -lbrotlicommon-static|" cgo.go
sed -i -e "/ -lm$/ n; /brotlienc/ s|$| -lm|" cgo.go

mkdir -p ${LIB_DIR}

cd ${LIB_DIR}
../configure-cmake --disable-debug
make

cd ${CWD}
make

rm -rf vendor/github.com/google/brotli/*
mv tmp/* vendor/github.com/google/brotli/
rm -rf tmp/

docker-compose build
docker-compose up --exit-code-from pg pg
