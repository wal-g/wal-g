#!/bin/sh


readonly CWD=$PWD

cd submodules/rocksdb
make static_lib

LDFLAGS=$(grep -oP 'PLATFORM_LDFLAGS=(.+)' make_config.mk | sed 's/^PLATFORM_LDFLAGS=\s\?//')" -lm -lstdc+"
echo $LDFLAGS

cd ${CWD}

mkdir -p tmp/rocksdb
cp -rf internal/databases/rocksdb/* tmp/rocksdb/
cd tmp/rocksdb

# patch cgo.go to force usage of static libraries for linking
sed -i -e 's/\#cgo CFLAGS:.*/\#cgo CFLAGS: -I..\/..\/submodules\/rocksdb\/include/' rocksdb.go
sed -i -e "s/\#cgo LDFLAGS:.*/\#cgo LDFLAGS: -L..\/..\/submodules\/rocksdb -lrocksdb\/ $LDFLAGS/" rocksdb.go

cd ${CWD}