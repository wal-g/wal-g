#!/bin/sh

set -e

test -d tmp/brotli || mkdir -p tmp/brotli

cp -rf vendor/github.com/google/brotli/* tmp/brotli/
cp -rf submodules/brotli/* vendor/github.com/google/brotli/

readonly CWD=$PWD

cd vendor/github.com/google/brotli/go/cbrotli

readonly LIB_DIR=../../dist  # dist will contain binaries and it is in the google/brotli/.gitignore

## patch cgo.go to force usage of static libraries for linking
cat <<EOF > cgo.go
// Copyright 2017 Google Inc. All Rights Reserved.
//
// Distributed under MIT license.
// See file LICENSE for detail or copy at https://opensource.org/licenses/MIT

package cbrotli

// Inform golang build system that it should link brotli libraries.

// #cgo CFLAGS: -I../../c/include
// #cgo LDFLAGS: -L../../dist -lbrotlidec-static -lbrotlicommon-static
// #cgo LDFLAGS: -L../../dist -lbrotlienc-static -lbrotlicommon-static -lm
import "C"
EOF

mkdir -p ${LIB_DIR}

cd ${LIB_DIR}
cmake ..
make

cd ${CWD}