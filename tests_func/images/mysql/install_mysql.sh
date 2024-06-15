#!/bin/sh
set -e -x

# Expect to have MYSQL_MAJOR in ['8.0', '5.7']

apt-get update
apt-get install lsb-release gnupg wget curl s3cmd jq

if [ "$MYSQL_MAJOR" = "8.0" ]; then
    wget https://repo.percona.com/apt/percona-release_latest.focal_all.deb
    dpkg -i percona-release_latest.focal_all.deb
    percona-release enable tools release
#    percona-release enable ps-80 release
    apt-get update
    apt-get install \
            mysql-server \
            mysql-client \
            percona-xtrabackup-80
else
  echo "FIXME: support MySQL 5.7?"
  exit 1
fi;

