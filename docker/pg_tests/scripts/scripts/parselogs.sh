#!/bin/bash
LOGS="${1}/*"
echo -e "\n\n"
cat ${LOGS} | head -n1 | egrep -o -e "wal-push" -e "wal-fetch" -e "backup-push" -e "backup-fetch"

cat ${LOGS} | grep "Elapsed (wall clock) time"

USER_TIME=`cat ${LOGS} | grep "User time (seconds):" | egrep -o "[0-9]+.[0-9]+"`

SYSTEM_TIME=`cat ${LOGS} | grep "System time (seconds):" | egrep -o "[0-9]+.[0-9]+"`

USER_PLUS_SYSTEM_TIME=`echo "$USER_TIME+$SYSTEM_TIME" | bc`
echo "User time + system time: $USER_PLUS_SYSTEM_TIME"