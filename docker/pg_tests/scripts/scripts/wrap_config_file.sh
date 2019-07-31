#!/bin/bash

touch /tmp/configs/new_config
echo "{" > /tmp/configs/new_config
cat $1 >> /tmp/configs/new_config
echo "" >> /tmp/configs/new_config
echo "}" >> /tmp/configs/new_config
cat /tmp/configs/new_config > $1
rm /tmp/configs/new_config