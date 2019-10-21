#!/bin/sh
set -e -x

for i in /tmp/tests/*; do
  ."$i";
  echo "${i} success"
done
