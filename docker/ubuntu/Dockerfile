FROM ubuntu:20.04

ENV DEBIAN_FRONTEND noninteractive
ENV TERM xterm-256color

RUN apt-get update && \
    apt-get install --yes --no-install-recommends --no-install-suggests \
    python3.8 \
    python3-setuptools \
    python3-pip \
    git \
    pv \
    wget \
    gnupg \
    lsb-release \
    htop \
    nano \
    liblzo2-2 \
    lzop \
    libbrotli1 && \
    rm -rf /var/lib/apt/lists/* && \
    python3.8 -m pip install --upgrade pip && \
    python3.8 -m pip install git+https://github.com/wal-e/wal-e.git && \
    python3.8 -m pip install boto
