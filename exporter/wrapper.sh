#!/bin/bash

# Simple wrapper script to pass --config to wal-g commands
# Usage: ./wrapper.sh [wal-g-command] [args...]

# Default config file path - change this as needed
CONFIG_FILE="${WALG_CONFIG_FILE:-/etc/wal-g/config.json}"

# Path to wal-g binary
WALG_BINARY="${WALG_BINARY:-wal-g}"

# Check if config file exists and add --config parameter
if [[ -f "$CONFIG_FILE" ]]; then
    exec "$WALG_BINARY" --config "$CONFIG_FILE" "$@"
else
    # If no config file, run without --config
    exec "$WALG_BINARY" "$@"
fi
