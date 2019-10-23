#!/bin/sh
/usr/bin/mysqlbinlog --defaults-file=/root/my.cnf --stop-datetime="$1" "$2" | /usr/bin/mysql --defaults-file=/tmp/my.cnf --port=3306