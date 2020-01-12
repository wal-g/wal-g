#!/bin/sh

if [ ! -f /tmp/recovery_state/gtid_purged ]; then
  /usr/bin/setter
fi
/usr/bin/mysqlbinlog - --stop-datetime="$UNTILL" | /usr/bin/mysql -u sbtest -h localhost