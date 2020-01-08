#!/bin/sh

test -d tmp/brotli || mkdir -p tmp/brotli

cp -rf vendor/github.com/google/brotli/* tmp/brotli/
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