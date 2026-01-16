#!/bin/bash
#
# Test script for WAL-G MySQL Exporter
# This script starts the exporter and tests its endpoints
#

set -e

EXPORTER_PORT=${EXPORTER_PORT:-9352}
EXPORTER_BIN="./walg-mysql-exporter"

echo "🚀 WAL-G MySQL Exporter Test Script"
echo "===================================="
echo ""

# Check if binary exists
if [ ! -f "$EXPORTER_BIN" ]; then
    echo "❌ Binary not found. Building..."
    make build
fi

# Start exporter in background
echo "📊 Starting exporter on port $EXPORTER_PORT..."
$EXPORTER_BIN \
    --web.listen-address=:$EXPORTER_PORT \
    --scrape.interval=10s \
    --walg.path=wal-g &

EXPORTER_PID=$!
echo "✅ Exporter started (PID: $EXPORTER_PID)"

# Trap to kill exporter on exit
trap "echo ''; echo '🛑 Stopping exporter...'; kill $EXPORTER_PID 2>/dev/null; exit" INT TERM EXIT

# Wait for exporter to start
echo "⏳ Waiting for exporter to be ready..."
sleep 3

# Test health endpoint
echo ""
echo "🔍 Testing /health endpoint..."
if curl -s "http://localhost:$EXPORTER_PORT/health" | grep -q "healthy"; then
    echo "✅ Health endpoint OK"
else
    echo "❌ Health endpoint failed"
fi

# Test metrics endpoint
echo ""
echo "🔍 Testing /metrics endpoint..."
if curl -s "http://localhost:$EXPORTER_PORT/metrics" | grep -q "walg_mysql"; then
    echo "✅ Metrics endpoint OK"
    echo ""
    echo "📈 Available metrics:"
    curl -s "http://localhost:$EXPORTER_PORT/metrics" | grep "^# HELP walg_mysql" | head -n 10
else
    echo "❌ Metrics endpoint failed"
fi

# Test root endpoint
echo ""
echo "🔍 Testing / endpoint..."
if curl -s "http://localhost:$EXPORTER_PORT/" | grep -q "WAL-G MySQL"; then
    echo "✅ Root endpoint OK"
else
    echo "❌ Root endpoint failed"
fi

echo ""
echo "✨ All tests passed!"
echo ""
echo "📊 Exporter running at: http://localhost:$EXPORTER_PORT"
echo "📈 Metrics available at: http://localhost:$EXPORTER_PORT/metrics"
echo "❤️  Health check at: http://localhost:$EXPORTER_PORT/health"
echo ""
echo "Press Ctrl+C to stop the exporter"
echo ""

# Keep script running
wait $EXPORTER_PID
