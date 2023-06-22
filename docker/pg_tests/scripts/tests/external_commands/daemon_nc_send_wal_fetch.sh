#!/bin/bash

# send binary message to wal-g daemon socket, details:
# internal/databases/postgres/daemon_handler.go

# [1] common message part:
# message type: 1 byte
# message body len: 2 bytes (len is including first 3 bytes)

# message body for wal-push:
# contain only WAL file name

# [2] message body for wal-fetch (and other multi-args commands):
# arg count: 1 byte
# for each arg:
#   - arg len: 2 byte
#   - arg content

if [[ "${2: -3}" == "LOG" ]]; then
  # example:
  # 00000001000000000000000A pg_wal/RECOVERYXLOG
  # message pars:
  #  - [1]: 3 bytes
  #  - [2]:
  #    - 1 (arg count, value = 2)
  #    - 2 + 24 (arg value len + arg value), 24 = x18 (hex)
  #    - 2 + 19 (arg value len + arg value), 19 = x13 (hex)
  # total message len: 51 = x33 (hex)
  {
    echo -en "f\x0\x33\x2"
    echo -en "\x0\x18${1}"
    echo -en "\x0\x13${2}"
  }  | nc -U "${WALG_SOCKET}"
else
  # example:
  # 00000002.history pg_wal/RECOVERYHISTORY
  # message pars:
  #  - [1]: 3 bytes
  #  - [2]:
  #    - 1 (arg count, value = 2)
  #    - 2 + 16 (arg value len + arg value), 16 = x10 (hex)
  #    - 2 + 22 (arg value len + arg value), 22 = x16 (hex)
  # total message len: 46 = x2E (hex)
  {
    echo -en "f\x0\x2E\x2"
    echo -en "\x0\x10${1}"
    echo -en "\x0\x16${2}"
  }  | nc -U "${WALG_SOCKET}"
fi
