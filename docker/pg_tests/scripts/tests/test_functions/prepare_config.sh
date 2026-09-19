#!/bin/sh

prepare_config() {
  if [ -z "${1}" ]; then
    echo "prepare_config should be run with test specific config file argument"
    exit 1
  fi

  CONFIG_FILE=$1
  COMMON_CONFIG="/tmp/configs/common_config.json"
  OUTPUT_CONFIG=${2:-/tmp/configs/tmp_config.json}
  if [ "$#" -lt 2 ]; then
    TMP_CONFIG="${OUTPUT_CONFIG}"
  fi
  jq -s '.[0] * .[1]' "${COMMON_CONFIG}" "${CONFIG_FILE}" > "${OUTPUT_CONFIG}"
}
