#!/usr/bin/env bash
pkill -9 mysqld
rm -rf "${MYSQLDATA}"
