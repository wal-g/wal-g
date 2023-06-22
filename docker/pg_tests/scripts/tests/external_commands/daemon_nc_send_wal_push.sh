#!/bin/bash

# see description in daemon_nc_send_wal_fetch.sh

echo -en "F\x0\x1B${1}" | nc -U "${WALG_SOCKET}"
