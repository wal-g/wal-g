FROM wal-g/base

ENV WALG_REDIS_DATA_FOLDER=/var/lib/redis
ENV REDIS_VERSION=7.2.5

RUN mkdir $WALG_REDIS_DATA_FOLDER

RUN apt-get clean && \
    apt-get update && apt-get install build-essential -y && \
    curl -fsSLO https://github.com/redis/redis/archive/refs/tags/${REDIS_VERSION}.tar.gz && \
    tar xzf "${REDIS_VERSION}.tar.gz" && \
    cd "redis-${REDIS_VERSION}" && \
    make install

COPY docker/redis/redis.conf /etc/redis/redis.conf
