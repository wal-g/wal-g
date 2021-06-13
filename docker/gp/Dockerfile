FROM wal-g/ubuntu:latest

RUN apt-get update && \
    apt-get install --yes --no-install-recommends --no-install-suggests locales iputils-ping ssh python-dev iproute2>
WORKDIR /usr/local
RUN git clone https://github.com/greenplum-db/gpdb.git gpdb_src

RUN ./gpdb_src/concourse/scripts/setup_gpadmin_user.bash

WORKDIR /usr/local/gpdb_src
RUN locale-gen en_US.utf8
RUN ./README.Ubuntu.bash

WORKDIR /usr/local/gpdb_src
RUN ./configure --with-perl --with-python --with-libxml --with-gssapi --prefix=/usr/local/gpdb_src > /dev/nul && \
    make -j8 > /dev/nul && \
    make -j8 install > /dev/null

ADD docker/gp/run_greenplum.sh /home/gpadmin/run_greenplum.sh
RUN chown gpadmin:gpadmin /home/gpadmin/run_greenplum.sh \
 && chmod a+x /home/gpadmin/run_greenplum.sh \
 && echo "export MASTER_DATA_DIRECTORY=/usr/local/gpdb_src/gpAux/gpdemo/datadirs/qddir/demoDataDir-1" > /home/gpadmi> && echo "source /usr/local/gpdb_src/greenplum_path.sh" > /home/gpadmin/.bash_profile && chown gpadmin:gpadmin /home>
RUN echo "gpadmin ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers \
 && echo "root ALL=NOPASSWD: ALL" >> /etc/sudoers