FROM wal-g/ubuntu:20.04

RUN apt-get update && \
    # required: \
    apt-get install --yes --no-install-recommends --no-install-suggests \
    build-essential \
    openssh-server openssh-client \
    iputils-ping \
    locales \
    python3-dev \
    pkg-config \
    rsync \
    && \
    # nice to have: \
    apt-get install --yes --no-install-recommends --no-install-suggests \
    iproute2 \
    less \
    sudo \
    gnupg \
    gpg-agent \
    pinentry-qt \
    time \
    bc \
    jq \
    wget

# https://github.com/yezzey-gp/ygp/blob/YGP_6.27_STABLE/README.ubuntu.bash (kind of)
RUN apt-get install --yes --no-install-recommends --no-install-suggests \
    bison \
    ccache \
    cmake \
    curl \
    flex \
    git-core \
    gcc \
    g++ \
    inetutils-ping \
    krb5-kdc \
    krb5-admin-server \
    libapr1-dev \
    libbz2-dev \
    libcurl4-gnutls-dev \
    libevent-dev \
    libkrb5-dev \
    libpam-dev \
    libperl-dev \
    libreadline-dev \
    libssl-dev \
    libxml2-dev \
    libyaml-dev \
    libzstd-dev \
    locales \
    net-tools \
    ninja-build \
    openssh-client \
    openssh-server \
    openssl \
    python3-dev \
    python3-pip \
    python3-psycopg2 \
    python3-psutil \
    python3-yaml \
    python3-pygresql \
    zlib1g-dev \
    libpstreams-dev \
    libxerces-c-dev

ADD docker/cloudberry/run_greenplum.sh /home/gpadmin/run_greenplum.sh

WORKDIR /usr/local

# You can run these tests with Greenplum 7.
# open-gpdb/gpdb doesn't have 7 branch... so, we can use adb-7:
#RUN git clone https://github.com/arenadata/gpdb.git gpdb_src --single-branch --branch adb-7.2.0 --depth 1 \
# && ./gpdb_src/concourse/scripts/setup_gpadmin_user.bash

RUN git clone https://github.com/cloudberrydb/cloudberrydb.git gpdb_src --single-branch --branch main --depth 1 \
 && ./gpdb_src/concourse/scripts/setup_gpadmin_user.bash

WORKDIR /usr/local/gpdb_src
RUN locale-gen en_US.utf8 \
 && git log -1 \
 && ./configure --with-perl --with-python --with-libxml --with-gssapi --prefix=/usr/local/gpdb_src --enable-debug --disable-pxf > /dev/null \
 && make -j$(nproc) > /dev/null \
 && make -j$(nproc) install > /dev/null \
 && chown gpadmin:gpadmin /home/gpadmin/run_greenplum.sh \
 && chmod a+x /home/gpadmin/run_greenplum.sh \
 && echo "export MASTER_DATA_DIRECTORY=/usr/local/gpdb_src/gpAux/gpdemo/datadirs/qddir/demoDataDir-1" > /home/gpadmin/.bash_profile \
 && echo "source /usr/local/gpdb_src/greenplum_path.sh" > /home/gpadmin/.bash_profile \
 && chown gpadmin:gpadmin /home/gpadmin/.bash_profile \
 && echo "gpadmin ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers \
 && echo "root ALL=NOPASSWD: ALL" >> /etc/sudoers \
 && echo "/usr/local/lib" >> /etc/ld.so.conf && ldconfig