#!/bin/bash

STATUS=`/usr/lib/postgresql/10/bin/pg_ctl status | egrep -o "server is running|no server running"`

while [[ "${STATUS}" != "server is running" ]]
do
  echo "Wait a sec to start server"
  sleep 1
  STATUS=`/usr/lib/postgresql/10/bin/pg_ctl status | egrep -o "server is running|no server running"`
done

echo "postgresql server is started"

STATUS_READ_ONLY=`echo "show transaction_read_only;" | psql | egrep -o "off"`

while [[ "${STATUS_READ_ONLY}" != "off" ]]
do
  echo "Wait a sec to not read-only mode"
  sleep 1
  echo "show transaction_read_only;" | psql postgres | egrep -o "off"
  STATUS_READ_ONLY=`echo "show transaction_read_only;" | psql | egrep -o "off"`
done
echo "postgresql read_only mode is off"
