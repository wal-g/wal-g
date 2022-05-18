FROM wal-g/ubuntu:latest

RUN apt-get update && \
    apt-get install --yes --no-install-recommends --no-install-suggests locales iputils-ping ssh python-dev iproute2 less sudo \
    gnupg \
    gpg-agent \
    pinentry-qt \
    time \
    bc \
    jq \
    wget

ADD docker/gp/run_greenplum.sh /home/gpadmin/run_greenplum.sh

WORKDIR /usr/local
RUN git clone https://github.com/greenplum-db/gpdb.git gpdb_src && cd gpdb_src && git checkout 6X_STABLE && cd .. \
 && ./gpdb_src/concourse/scripts/setup_gpadmin_user.bash

WORKDIR /usr/local/gpdb_src
RUN locale-gen en_US.utf8 \
 && sed -i 's/apt-get install/DEBIAN_FRONTEND=noninteractive apt-get install/g' README.ubuntu.bash \
 && ./README.ubuntu.bash \
 && wget -c https://archive.apache.org/dist/xerces/c/3/sources/xerces-c-3.1.1.tar.gz -O - | tar -xz \
 && cd xerces-c-3.1.1 && ./configure && make -j8 > /dev/null && make -j8 install && cd .. \
 && ./configure --with-perl --with-python --with-libxml --with-gssapi --prefix=/usr/local/gpdb_src > /dev/null \
 && make -j8 > /dev/null \
 && make -j8 install > /dev/null \
 && chown gpadmin:gpadmin /home/gpadmin/run_greenplum.sh \
 && chmod a+x /home/gpadmin/run_greenplum.sh \
 && echo "export MASTER_DATA_DIRECTORY=/usr/local/gpdb_src/gpAux/gpdemo/datadirs/qddir/demoDataDir-1" > /home/gpadmin/.bash_profile \
 && echo "source /usr/local/gpdb_src/greenplum_path.sh" > /home/gpadmin/.bash_profile \
 && chown gpadmin:gpadmin /home/gpadmin/.bash_profile \
 && echo "gpadmin ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers \
 && echo "root ALL=NOPASSWD: ALL" >> /etc/sudoers \
 && echo "/usr/local/lib" >> /etc/ld.so.conf && ldconfig