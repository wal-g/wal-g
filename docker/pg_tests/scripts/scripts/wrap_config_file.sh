#!/bin/bash
TMP_CONFIG="/tmp/configs/new_config"

touch ${TMP_CONFIG}
echo "{" > ${TMP_CONFIG}
cat $1 >> ${TMP_CONFIG}
echo "}" >> ${TMP_CONFIG}
cat ${TMP_CONFIG} > $1
rm ${TMP_CONFIG}