#!/bin/bash

DATABASE=$1
TABLE=$2
PORT=$3

ROW_COUNT=`psql --port ${PORT} -d ${DATABASE} -c "SELECT COUNT(*) from ${TABLE}" | grep -E '[0-9]+' | head -1`

while [[ "${ROW_COUNT}" != " 10000000" ]]
do
  echo "Wait a sec to replication end"
  sleep 1
  ROW_COUNT=`psql --port ${PORT} -c "SELECT COUNT(*) from ${TABLE}" | grep -E '[0-9]+' | head -1`
done
