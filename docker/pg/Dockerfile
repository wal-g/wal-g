FROM wal-g/ubuntu:latest

ENV PGDATA /var/lib/postgresql/10/main

RUN apt-get update && \
    apt-get install --yes --no-install-recommends --no-install-suggests \
    postgresql-10 \
    postgresql-contrib

COPY docker/pg/walg.json /tmp/walg.json

RUN rm -rf $PGDATA
