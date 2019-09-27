FROM wal-g/ubuntu:latest

ENV PGDATA /var/lib/postgresql/10/main

RUN apt-get update && \
    apt-get install --yes --no-install-recommends --no-install-suggests \
    postgresql-10 \
    postgresql-contrib \
    gnupg \
    gpg-agent \
    pinentry-qt \
    time \
    bc \
    jq

COPY docker/pg/PGP_KEY /tmp/PGP_KEY
COPY docker/pg/gpg.conf /var/lib/postgresql/.gnupg/gpg.conf
COPY docker/pg/gpg-agent.conf /var/lib/postgresql/.gnupg/gpg-agent.conf

COPY docker/pg/walg.json /tmp/walg.json

RUN rm -rf $PGDATA

RUN chmod 777 /var/lib/postgresql/.gnupg