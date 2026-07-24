FROM debian:bullseye-slim

ENV DEBIAN_FRONTEND=noninteractive
ENV TERM=xterm-256color

COPY docker/ubuntu/config/apt/ /var/lib/dist/config/apt
RUN cp --force --recursive /var/lib/dist/config/apt/* /etc/apt/

RUN apt-get update && \
    apt-get install --yes --no-install-recommends --no-install-suggests \
    git ca-certificates liblzo2-2 curl procps diffutils && \
    rm -rf /var/lib/apt/lists/*
