#!/bin/bash

set -e

readonly CWD=$PWD
readonly OS=$(uname)
readonly LIBSODIUM_VERSION=${LIBSODIUM_VERSION:-1.0.20}

test -d tmp/libsodium || mkdir -p tmp/libsodium

cd tmp/libsodium

# In isolated build environments (e.g., for packaging or Launchpad),
# we must include the libsodium tarball in our source archive.
# This check ensures we reuse the local tarball if it already exists,
# or download it from GitHub for regular workflows.
if [ ! -f "libsodium-$LIBSODIUM_VERSION.tar.gz" ]; then
  curl --retry 5 --retry-delay 0 -sL "https://github.com/jedisct1/libsodium/releases/download/${LIBSODIUM_VERSION}-RELEASE/libsodium-${LIBSODIUM_VERSION}.tar.gz" -o "libsodium-${LIBSODIUM_VERSION}.tar.gz"
fi

tar xfz libsodium-$LIBSODIUM_VERSION.tar.gz --strip-components=1

CONFIGURE_ARGS="--prefix ${PWD}"
if [[ "${OS}" == "SunOS" ]]; then
  # On Illumos / Solaris libssp causes linking issues when building wal-g.
  CONFIGURE_ARGS="${CONFIGURE_ARGS} --disable-ssp"
fi      

./configure ${CONFIGURE_ARGS}
make && make check && make install

# Remove shared libraries for using static
rm -f lib/*.so lib/*.so.* lib/*.dylib

cd ${CWD}
