#!/bin/sh

readonly CWD=$PWD
readonly LIBSODIUM_VERSION="libsodium-1.0.17"

test -d tmp/libsodium || mkdir -p tmp/libsodium

cd tmp/libsodium

curl -sL https://download.libsodium.org/libsodium/releases/$LIBSODIUM_VERSION.tar.gz -o $LIBSODIUM_VERSION.tar.gz
tar xfz $LIBSODIUM_VERSION.tar.gz --strip-components=1

./configure --prefix $PWD
make && make check && make install

# Remove shared libraries for using static
rm lib/*.so lib/*.so.*

cd ${CWD}
