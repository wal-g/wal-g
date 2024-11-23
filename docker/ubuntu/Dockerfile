FROM ubuntu:18.04

ENV DEBIAN_FRONTEND noninteractive
ENV TERM xterm-256color

RUN apt-get update && \
    apt-get install --yes --no-install-recommends --no-install-suggests \
    git ca-certificates \
    htop \
    nano \
    liblzo2-2 && \
    rm -rf /var/lib/apt/lists/*