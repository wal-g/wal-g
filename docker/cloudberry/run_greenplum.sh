#!/bin/bash

set +ex

sudo /etc/init.d/ssh start

source /usr/local/gpdb_src/cloudberry-env.sh

cd /usr/local/gpdb_src

/usr/local/gpdb_src/bin/gpssh-exkeys -h `hostname`
