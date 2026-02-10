#!/bin/bash
# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  WAL-G Aware MariaDB Entrypoint                                             ║
# ║  Handles: Restore → Initialize → Start → Optional PITR                     ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

set -eo pipefail

DATADIR="${MYSQL_DATADIR:-/var/lib/mysql}"
MYSQL_USER="${MYSQL_USER:-mysql}"
MYSQL_GROUP="${MYSQL_GROUP:-mysql}"

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PHASE 1: RESTORE FROM BACKUP (if needed)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

if [ ! -d "$DATADIR/mysql" ]; then
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "Empty DATADIR detected: $DATADIR"
    
    if [ -n "$WALG_RESTORE_FROM_BACKUP" ]; then
        echo "Restoring from backup: $WALG_RESTORE_FROM_BACKUP"
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        
        # Create temp restore directory
        RESTORE_TEMP="/tmp/walg-restore-$$"
        mkdir -p "$RESTORE_TEMP"
        
        # Fetch backup
        echo "Downloading backup from storage..."
        /usr/local/bin/wal-g backup-fetch "$WALG_RESTORE_FROM_BACKUP" "$RESTORE_TEMP"
        
        # Move to datadir
        echo "Moving data to $DATADIR..."
        rm -rf "$DATADIR"
        mv "$RESTORE_TEMP" "$DATADIR"
        
        # Fix permissions
        echo "Applying permissions $MYSQL_USER:$MYSQL_GROUP..."
        chown -R "$MYSQL_USER:$MYSQL_GROUP" "$DATADIR"
        chmod 750 "$DATADIR"
        
        echo "Backup restored successfully"
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    else
        echo "WALG_RESTORE_FROM_BACKUP not configured"
        echo "MariaDB will initialize normally"
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    fi
fi

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PHASE 2: START MARIADB
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

echo "Starting MariaDB..."

# Start MariaDB in background
/usr/local/bin/docker-entrypoint.sh "$@" &
MARIADB_PID=$!

echo "MariaDB PID: $MARIADB_PID"

# Wait for MariaDB to be ready
echo "Waiting for MariaDB to be ready..."
for i in {1..30}; do
    if mysqladmin ping --silent; then
        echo "MariaDB is ready"
        break
    fi
    if ! kill -0 $MARIADB_PID 2>/dev/null; then
        echo "MariaDB failed to start"
        exit 1
    fi
    sleep 1
done

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PHASE 3: PITR (if requested)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

if [ -n "$WALG_PITR_UNTIL" ]; then
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "PITR requested until: $WALG_PITR_UNTIL"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    # Stop binary logging to prevent new logs during replay
    mysql -e "SET GLOBAL log_bin=OFF;" 2>/dev/null || true
    
    # Execute binlog-replay
    echo "Applying binlogs until $WALG_PITR_UNTIL..."
    /usr/local/bin/wal-g binlog-replay \
        --since="${WALG_PITR_SINCE:-LATEST}" \
        --until="$WALG_PITR_UNTIL"
    
    echo "PITR completed"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
fi

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PHASE 4: KEEP MARIADB RUNNING
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Setup signal handlers
shutdown_mariadb() {
    echo "Received shutdown signal..."
    
    # Try graceful shutdown first
    if [ -n "${MYSQL_ROOT_PASSWORD}" ]; then
        mysqladmin -uroot -p"${MYSQL_ROOT_PASSWORD}" shutdown 2>/dev/null || true
    fi
    
    # Send SIGTERM to MariaDB
    kill -TERM $MARIADB_PID 2>/dev/null || true
    
    # Wait up to 30 seconds
    for i in {1..30}; do
        if ! kill -0 $MARIADB_PID 2>/dev/null; then
            echo "MariaDB stopped gracefully"
            exit 0
        fi
        sleep 1
    done
    
    # Force kill if still running
    echo "Forcing shutdown..."
    kill -KILL $MARIADB_PID 2>/dev/null || true
    exit 1
}

trap shutdown_mariadb SIGTERM SIGINT

# Wait for MariaDB process
echo "System ready - MariaDB running"
wait $MARIADB_PID
EXIT_CODE=$?

echo "MariaDB exited with code: $EXIT_CODE"
exit $EXIT_CODE
