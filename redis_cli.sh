#!/usr/bin/env bash
#
# All args passed to this script will be passed to "redis-cli .. --rdb -"
#
# This script - workaround with redis-cli >= 6.2 bug, when redis-cli tries to fsync /dev/stdout and exit with error
# see https://github.com/redis/redis/pull/9135
#

FILENAME=$(mktemp --suffix=redis-cli-stderr)
redis-cli $@ --rdb - 2>$FILENAME
exit_code=$?
cat $FILENAME >&2
grep "Fail to fsync" $FILENAME | grep -q "Invalid argument"
FAILED=$?
rm $FILENAME;
if [[ $exit_code -ne 0 ]] ; then
    test 0 -eq $FAILED || exit $exit_code
fi
exit 0
