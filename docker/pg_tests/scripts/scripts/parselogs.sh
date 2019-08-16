#!/bin/bash
LOGS="${1}/*"
echo "Current directory - ${1}"

echo "Elapsed (wall clock) time (m:ss)"
cat ${LOGS} | grep "Elapsed (wall clock) time" | egrep -o -e "[0-9]+:[0-9]+.[0-9]+"| sort | tail -n1

echo "User time (seconds):"
cat ${LOGS} | grep "User time (seconds):" | egrep -o "[0-9]+.[0-9]+" | sort -n | tail -n1

echo "System time (seconds):"
cat ${LOGS} | grep "System time (seconds):" | egrep -o "[0-9]+.[0-9]+" | sort -n | tail -n1

echo "Percent of CPU this job got:"
cat ${LOGS} | grep "Percent of CPU this job got" | egrep -o "[0-9]+" | sort -n | tail -n1