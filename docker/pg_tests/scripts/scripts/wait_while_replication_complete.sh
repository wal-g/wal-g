#!/bin/bash

DATABASE=$1
TABLE=$2
PORT=$3
ROW_COUNT_EXCPECTED=" $4"

ROW_COUNT=`psql --port ${PORT} -d ${DATABASE} -c "SELECT COUNT(*) from ${TABLE}" | grep -E '[0-9]+' | head -1`

while [[ "${ROW_COUNT}" != "${ROW_COUNT_EXCPECTED}" ]]
do
  echo "Wait a sec to replication end"
  sleep 1
  ROW_COUNT=`psql --port ${PORT} -c "SELECT COUNT(*) from ${TABLE}" | grep -E '[0-9]+' | head -1`
done

# Wait more time just in case, because we look only one table
sleep 5
