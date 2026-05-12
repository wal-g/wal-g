ARG UBUNTU_TAG=18.04
FROM wal-g/ubuntu:${UBUNTU_TAG}

ARG PG_MAJOR=10
ARG USE_PGDG=0
ARG INSTALL_WALE=1

ENV PG_MAJOR=${PG_MAJOR}
ENV PGDATA=/var/lib/postgresql/${PG_MAJOR}/main
ENV PATH=/usr/lib/postgresql/${PG_MAJOR}/bin:$PATH

RUN apt-get update && \
    apt-get install --yes --no-install-recommends --no-install-suggests \
    gnupg gpg-agent ca-certificates && \
    if [ "${USE_PGDG}" = "1" ]; then \
        apt-get install --yes --no-install-recommends --no-install-suggests lsb-release && \
        install -d /usr/share/postgresql-common/pgdg && \
        curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
            -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc && \
        echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" \
            > /etc/apt/sources.list.d/pgdg.list && \
        apt-get update; \
    fi && \
    apt-get install --yes --no-install-recommends --no-install-suggests \
    postgresql-${PG_MAJOR} \
    postgresql-contrib-${PG_MAJOR} \
    time \
    bc \
    jq \
    s3cmd \
    lzop \
    pv

RUN if [ "${INSTALL_WALE}" = "1" ]; then \
        apt-get install --yes --no-install-recommends --no-install-suggests \
            python3.7 python3-setuptools python3-pip && \
        python3.7 -m pip install --upgrade pip && \
        python3.7 -m pip install git+https://github.com/wal-e/wal-e.git && \
        python3.7 -m pip install boto; \
    fi

COPY docker/pg/PGP_KEY /tmp/PGP_KEY
COPY docker/pg/SSH_KEY /tmp/SSH_KEY
COPY docker/pg/gpg.conf /var/lib/postgresql/.gnupg/gpg.conf
COPY docker/pg/gpg-agent.conf /var/lib/postgresql/.gnupg/gpg-agent.conf

COPY docker/common/s3cfg /var/lib/postgresql/.s3cfg

COPY docker/pg/walg.json /tmp/walg.json

RUN rm -rf $PGDATA

RUN chmod 777 /var/lib/postgresql/.gnupg
