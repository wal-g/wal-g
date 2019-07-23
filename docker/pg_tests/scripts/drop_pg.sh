#!/bin/bash
pkill -9 postgres || true
pkill -9 wal-g || true
rm -rf $PGDATA
