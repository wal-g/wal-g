FROM wal-g/ubuntu:18.04

ENV WALG_REDIS_DATA_FOLDER /var/lib/redis

RUN mkdir $WALG_REDIS_DATA_FOLDER

RUN apt-get update && \
    apt-get install --yes --no-install-recommends --no-install-suggests \
    redis-server

COPY docker/redis/redis.conf /etc/redis/redis.conf