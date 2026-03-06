#!/usr/bin/env bash

# common wal-g settings
export WALG_MYSQL_DATASOURCE_NAME=sbtest:@/sbtest
export WALG_STREAM_CREATE_COMMAND="mariabackup --backup --stream=xbstream --user=sbtest --host=localhost --datadir=${MYSQLDATA}"
export WALG_STREAM_RESTORE_COMMAND="mbstream -x -C ${MYSQLDATA}"
export WALG_MYSQL_BACKUP_PREPARE_COMMAND="mariabackup --prepare --target-dir=${MYSQLDATA}"
export WALG_MYSQL_CHECK_GTIDS=False
export WALG_COMPRESSION_METHOD=zstd


# test tools
mariadb_kill_and_clean_data() {
    local max_attempts=10
    local attempt=0
    while [ "$attempt" -lt "$max_attempts" ]; do
        if service mariadb stop; then
            break
        fi
        service mysql stop 2>/dev/null || true
        attempt=$((attempt + 1))
        echo "Stopping MariaDB... Attempt $attempt/$max_attempts"
        sleep 1
    done
    if [ "$attempt" -eq "$max_attempts" ]; then
        echo "WARNING: Could not stop MariaDB gracefully, forcing kill"
        pkill -9 mariadbd 2>/dev/null || pkill -9 mysqld 2>/dev/null || true
        sleep 2
    fi

    rm -rf "${MYSQLDATA}"/*
    rm -rf "${MYSQLDATA}"/.tmp
    rm -rf /root/.walg_mysql_binlogs_cache
}

mariadb_installdb() {
    mysql_install_db --user=mysql --datadir="${MYSQLDATA}" | grep -v '^$'
    chown -R mysql:mysql "${MYSQLDATA}"
    echo "MariaDB version: $(mariadb --version)"
}

mariadb_get_version() {
    mariadb --version | grep -oP '(?<=Distrib )[\d\.]+'
}

mariadb_version_check() {
    local required_version="$1"
    local current_version
    current_version=$(mariadb_get_version)

    if [ "$(printf '%s\n' "$required_version" "$current_version" | sort -V | head -n1)" != "$required_version" ]; then
        echo "SKIP: MariaDB $current_version < $required_version (required)"
        return 1
    fi
    return 0
}

sysbench() {
    # shellcheck disable=SC2068
    /usr/bin/sysbench --db-driver=mysql --verbosity=0 /usr/share/sysbench/oltp_insert.lua $@
}

date3339() {
    date --rfc-3339=ns | sed 's/ /T/'
}

mysql_set_gtid_from_backup() {
    local gtid_file="${MYSQLDATA}/xtrabackup_binlog_info"

    if [ ! -f "$gtid_file" ]; then
        gtid_file="${MYSQLDATA}/mariadb_backup_binlog_info"
    fi

    if [ ! -f "$gtid_file" ]; then
        echo "ERROR: GTID file not found (tried xtrabackup_binlog_info and mariadb_backup_binlog_info)"
        return 1
    fi

    local gtids
    gtids=$(tail -n 1 < "$gtid_file" | awk '{print $3}')

    if [ -z "$gtids" ]; then
        echo "ERROR: Could not extract GTIDs from backup"
        return 1
    fi

    echo "Setting GTIDs from backup: $gtids"
    mysql -e "STOP ALL SLAVES; SET GLOBAL gtid_slave_pos='$gtids';" 2>/dev/null || \
    mysql -e "SET GLOBAL gtid_slave_pos='$gtids';"
}
