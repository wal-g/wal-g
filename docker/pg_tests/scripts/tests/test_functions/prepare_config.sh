#!/bin/sh

prepare_config() {
  if [ -z "${1}" ]; then
    echo "prepare_config should be run with test specific config file argument"
    exit 1
  fi

  CONFIG_FILE=$1
  COMMON_CONFIG="/tmp/configs/common_config.json"
  TMP_CONFIG="/tmp/configs/tmp_config.json"
  cat "${CONFIG_FILE}" > "${TMP_CONFIG}"
  echo "," >> "${TMP_CONFIG}"
  cat "${COMMON_CONFIG}" >> "${TMP_CONFIG}"
  /tmp/scripts/wrap_config_file.sh "${TMP_CONFIG}"
}