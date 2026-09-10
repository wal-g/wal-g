#!/bin/sh
set -e -x

rm -rf "${PGDATA}"

# shellcheck disable=SC1091
. /tmp/tests/test_functions/prepare_config.sh
prepare_config "/tmp/configs/wal_perftest_throttling_config.json"

FAULT_ADMIN="http://s3-for-throttling:9901"

set_fault_percentage() {
    percentage="$1"
    curl -fsS -X POST -o /dev/null \
        "${FAULT_ADMIN}/runtime_modify?fault.http.abort.abort_percent=${percentage}&fault.http.delay.fixed_delay_percent=${percentage}"
}

enable_faults() {
    http_status="$1"
    curl -fsS -X POST -o /dev/null \
        "${FAULT_ADMIN}/runtime_modify?fault.http.abort.http_status=${http_status}&fault.http.abort.abort_percent=100&fault.http.delay.fixed_delay_percent=100"
}

fault_stat() {
    stat_name="$1"
    curl -fsS "${FAULT_ADMIN}/stats?filter=http.s3_proxy.fault.${stat_name}$" \
        | awk -F': ' -v stat_name="${stat_name}" '$1 ~ ("\\." stat_name "$") { print $2 }'
}

disable_faults() {
    set_fault_percentage 0
}

wait_for_fault_proxy() {
    attempt=0
    until curl -fsS -o /dev/null "${FAULT_ADMIN}/ready"; do
        attempt=$((attempt + 1))
        test "${attempt}" -lt 100
        sleep 0.1
    done
}

run_with_one_fault() {
    http_status="$1"
    shift

    before_delays=$(fault_stat delays_injected)
    before_aborts=$(fault_stat aborts_injected)
    expected_aborts=$((before_aborts + 1))

    enable_faults "${http_status}"
    "$@" &
    operation_pid=$!

    # Envoy evaluates the abort percentage after the configured delay expires.
    # Keep faults enabled until the abort is recorded; disabling them while the
    # delay is active lets the request pass without the expected HTTP error.
    attempt=0
    current_aborts=$(fault_stat aborts_injected)
    while [ "${current_aborts}" -lt "${expected_aborts}" ]; do
        attempt=$((attempt + 1))
        if [ "${attempt}" -ge 100 ]; then
            disable_faults
            wait "${operation_pid}" || true
            echo "Envoy did not inject an abort" >&2
            return 1
        fi
        sleep 0.05
        current_aborts=$(fault_stat aborts_injected)
    done

    disable_faults
    wait "${operation_pid}"

    after_delays=$(fault_stat delays_injected)
    after_aborts=$(fault_stat aborts_injected)
    test "${after_delays}" -ge $((before_delays + 1))
    test "${after_aborts}" -eq "${expected_aborts}"
}

wait_for_fault_proxy
disable_faults
trap 'disable_faults || true' EXIT
trap 'exit 1' INT TERM

initdb "${PGDATA}"
pg_ctl -D "${PGDATA}" -w start

wal-g --config="${TMP_CONFIG}" delete everything FORCE --confirm

pgbench -i -s 50 postgres
pgbench -c 100 -t 100 postgres

du -hs "${PGDATA}" 2>/dev/null || true
WAL=$(psql -At -c 'SELECT pg_walfile_name(pg_switch_wal());' postgres)

du -hs "${PGDATA}" 2>/dev/null || true

# Prove that the S3 retryer recovers from each supported delayed HTTP error for
# both upload and download before running the high-concurrency workload.
for FAULT_STATUS in 409 429 503 504; do
    FAULT_WAL="${WAL}${FAULT_STATUS}"
    FAULT_WAL_PATH="${PGDATA}/pg_wal/${FAULT_WAL}"
    FAULT_FETCH_PATH="/tmp/fault-wal-fetch-${FAULT_STATUS}"

    cp "${PGDATA}/pg_wal/${WAL}" "${FAULT_WAL_PATH}"
    run_with_one_fault "${FAULT_STATUS}" wal-g --config="${TMP_CONFIG}" wal-push "${FAULT_WAL_PATH}"
    run_with_one_fault "${FAULT_STATUS}" wal-g --config="${TMP_CONFIG}" wal-fetch "${FAULT_WAL}" "${FAULT_FETCH_PATH}"
    cmp "${FAULT_WAL_PATH}" "${FAULT_FETCH_PATH}"
done

i=0
START=$(date +%s)
PUSH_PIDS=""
while [ "$i" -le 101 ];
do
    cp "${PGDATA}/pg_wal/${WAL}" "${PGDATA}/pg_wal/${WAL}${i}"
    cp "${PGDATA}/pg_wal/${WAL}" "${PGDATA}/pg_wal/${i}${WAL}"
    cp "${PGDATA}/pg_wal/${WAL}" "${PGDATA}/pg_wal/${i}${WAL}${i}"
    wal-g --config="${TMP_CONFIG}" wal-push "${PGDATA}/pg_wal/${WAL}${i}" &
    PUSH_PIDS="${PUSH_PIDS} $!"
    wal-g --config="${TMP_CONFIG}" wal-push "${PGDATA}/pg_wal/${i}${WAL}" &
    PUSH_PIDS="${PUSH_PIDS} $!"
    wal-g --config="${TMP_CONFIG}" wal-push "${PGDATA}/pg_wal/${i}${WAL}${i}" &
    PUSH_PIDS="${PUSH_PIDS} $!"
    i=$(( i + 1 ))
done
PUSH_FAILED=0
for push_pid in ${PUSH_PIDS}; do
    if ! wait "${push_pid}"; then
        PUSH_FAILED=1
    fi
done
test "${PUSH_FAILED}" -eq 0
END=$(date +%s)
DIFF=$((END - START))
echo "It took $DIFF seconds"
test "$DIFF" -le 100
/tmp/scripts/drop_pg.sh

i=0
while [ "$i" -le 101 ];
do
    wal-g --config="${TMP_CONFIG}" wal-fetch "${WAL}${i}" "${PGDATA}${i}"
    wal-g --config="${TMP_CONFIG}" wal-fetch "${i}${WAL}" "${PGDATA}${i}A"
    wal-g --config="${TMP_CONFIG}" wal-fetch "${i}${WAL}${i}" "${PGDATA}${i}B"
    i=$(( i + 1 ))
done
sleep 1
/tmp/scripts/drop_pg.sh
disable_faults
trap - EXIT INT TERM
