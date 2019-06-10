#!/bin/bash
set -e -x

pushd /tmp
for i in tests/*; do ./$i; done
popd
