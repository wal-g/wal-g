#!/bin/bash

sudo /etc/init.d/ssh start

source /usr/local/gpdb_src/greenplum_path.sh

cd /usr/local/gpdb_src

/usr/local/gpdb_src/bin/gpssh-exkeys -h `hostname`
