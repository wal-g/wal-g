#!/bin/bash
set -e -x

pushd /tmp
for i in tests/*.sh; do ./$i; done
popd
