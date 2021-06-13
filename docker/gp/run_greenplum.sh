#!/bin/bash

sudo /etc/init.d/ssh start

. /usr/local/gpdb_src/greenplum_path.sh

cd /usr/local/gpdb_src

/usr/local/gpdb_src/bin/gpssh-exkeys -h `hostname`

make create-demo-cluster

. /usr/local/gpdb_src/gpAux/gpdemo/gpdemo-env.sh