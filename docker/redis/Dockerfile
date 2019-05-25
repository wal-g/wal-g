FROM wal-g/ubuntu:latest

ENV REDISDATA /var/lib/redis

RUN apt-get update && \
    apt-get install --yes --no-install-recommends --no-install-suggests \
    redis-server


RUN rm -rf $REDISDATA