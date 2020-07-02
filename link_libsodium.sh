#!/bin/sh

set -e

readonly CWD=$PWD
readonly LIBSODIUM_VERSION="1.0.17"

test -d tmp/libsodium || mkdir -p tmp/libsodium

cd tmp/libsodium

curl --retry 5 --retry-delay 0 -sL https://github.com/jedisct1/libsodium/releases/download/$LIBSODIUM_VERSION/libsodium-$LIBSODIUM_VERSION.tar.gz -o libsodium-$LIBSODIUM_VERSION.tar.gz
tar xfz libsodium-$LIBSODIUM_VERSION.tar.gz --strip-components=1

./configure --prefix $PWD
make && make check && make install

# Remove shared libraries for using static
rm -f lib/*.so lib/*.so.* lib/*.dylib

cd ${CWD}
