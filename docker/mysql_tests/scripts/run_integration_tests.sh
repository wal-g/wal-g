#!/bin/sh
set -e

if [ "$#" -ne 0 ]; then
    echo "Usage: MYSQL_TEST_FILTER='<filename substring>' $0" >&2
    exit 2
fi

scripts_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
test_filter=${MYSQL_TEST_FILTER:-}

# Select all integration groups; utils contains helpers, not integration tests.
# Keep each selected path as a separate argument.
set --
for test_file in "$scripts_dir"/*_tests/*.sh; do
    [ -f "$test_file" ] || continue
    case "${test_file##*/}" in
        *"$test_filter"*) set -- "$@" "$test_file" ;;
    esac
done

if [ "$#" -eq 0 ]; then
    echo "No MySQL integration tests match MYSQL_TEST_FILTER='$test_filter'" >&2
    exit 1
fi

. /usr/local/export_common.sh

sh "$scripts_dir/utils/mysql_helpers_test.sh"

# Clean up data left by a previous unsuccessful test run.
mysql_kill_and_clean_data

printf 'Running %s MySQL integration tests (filter: %s)\n' "$#" "${test_filter:-all}"
for test_file do
    echo
    echo "===== RUNNING $test_file ====="
    set -x
    chmod a+x "$test_file"
    timeout 10m "$test_file"
    set +x
    echo "===== SUCCESS $test_file ====="
    echo
    mysql_kill_and_clean_data
done
