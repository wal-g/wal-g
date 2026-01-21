#!/bin/bash
# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  Binlog Replay Helper for WAL-G                                            ║
# ║  Called by WAL-G with WALG_MYSQL_CURRENT_BINLOG and                        ║
# ║  WALG_MYSQL_BINLOG_END_TS environment variables                            ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

set -eo pipefail

if [ -z "$WALG_MYSQL_CURRENT_BINLOG" ]; then
    echo "ERROR: WALG_MYSQL_CURRENT_BINLOG not set" >&2
    exit 1
fi

if [ ! -f "$WALG_MYSQL_CURRENT_BINLOG" ]; then
    echo "ERROR: Binlog file not found: $WALG_MYSQL_CURRENT_BINLOG" >&2
    exit 1
fi

echo "Replaying binlog: $(basename "$WALG_MYSQL_CURRENT_BINLOG")"

# Build mysqlbinlog command
MYSQLBINLOG_CMD="mysqlbinlog"

# Add stop-datetime if provided
if [ -n "$WALG_MYSQL_BINLOG_END_TS" ]; then
    MYSQLBINLOG_CMD="$MYSQLBINLOG_CMD --stop-datetime='$WALG_MYSQL_BINLOG_END_TS'"
    echo "Stop datetime: $WALG_MYSQL_BINLOG_END_TS"
fi

# Add binlog file
MYSQLBINLOG_CMD="$MYSQLBINLOG_CMD '$WALG_MYSQL_CURRENT_BINLOG'"

# Build mysql command
MYSQL_CMD="mysql"

if [ -n "$MYSQL_ROOT_PASSWORD" ]; then
    MYSQL_CMD="$MYSQL_CMD -uroot -p'$MYSQL_ROOT_PASSWORD'"
elif [ -n "$MYSQL_USER" ] && [ -n "$MYSQL_PASSWORD" ]; then
    MYSQL_CMD="$MYSQL_CMD -u'$MYSQL_USER' -p'$MYSQL_PASSWORD'"
fi

# Execute
eval "$MYSQLBINLOG_CMD | $MYSQL_CMD"

echo "Binlog replayed successfully"
