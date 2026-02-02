#!/bin/sh
set -e -x

. /tmp/tests/test_functions/prepare_config.sh
. /tmp/tests/test_functions/test_copy_composer.sh

prepare_config "/tmp/configs/full_backup_copy_composer_test_config.json"
test_copy_composer ${TMP_CONFIG}

/tmp/scripts/drop_pg.sh
