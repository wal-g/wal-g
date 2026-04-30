#!/bin/bash
set -euo pipefail

# This is a mock script for wal-g to be used in integration tests.
# It mimics the output of wal-g commands based on the arguments provided.

# The first argument is the command, e.g., "backup-list"
COMMAND=$1

case "$COMMAND" in
  "backup-list")
    if [[ "$2" == "--detail" && "$3" == "--json" ]]; then
      # Mock backup-list
      # sleep $(( $((RANDOM % 2)) + 1 )).$(( $((RANDOM % 9)) + 1 ))
      cat ./testdata/backup-list.json
      exit 0
    fi
    ;;
  "wal-verify")
    if [[ "$2" == "integrity" && "$3" == "timeline" && "$4" == "--json" ]]; then
      # Mock wal-verify integrity timeline
      # sleep $(( $((RANDOM % 4)) + 1 )).$(( $((RANDOM % 9)) + 1 ))
      cat ./testdata/wal-verify.json
      exit 0
    fi
    ;;
  "st")
    if [[ "$2" == "check" && "$3" == "read" ]]; then
      # Mock storage check - simulate successful storage connectivity
      # sleep 0.$(( $((RANDOM % 10)) + 1 ))
      echo "Read check OK"
      exit 0
    fi
    ;;
esac

echo "Unknown mock command: $COMMAND $*" >&2
exit 1
