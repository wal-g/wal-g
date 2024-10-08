#!/bin/sh

set -e

test -d tmp/brotli || mkdir -p tmp/brotli

cp -rf vendor/github.com/google/brotli/* tmp/brotli/
cp -rf submodules/brotli/* vendor/github.com/google/brotli/

readonly CWD=$PWD

cd vendor/github.com/google/brotli/go/cbrotli

readonly LIB_DIR=../../dist  # dist will contain binaries and it is in the google/brotli/.gitignore

mkdir -p ${LIB_DIR}

cd ${LIB_DIR}
cmake -DBUILD_SHARED_LIBS=0 ..
make

cd ${CWD}
