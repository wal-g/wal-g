# vim:set ft=dockerfile:
FROM golang:1.22.8-bullseye

ENV DEBIAN_FRONTEND noninteractive

# Configure apt and locale
RUN apt-get update -q && \
    apt-get install --yes --no-install-recommends --no-install-suggests \
    cmake build-essential && \
    rm -rf /var/lib/apt/lists/*
