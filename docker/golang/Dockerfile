FROM golang:bullseye

ENV DEBIAN_FRONTEND noninteractive
ENV TERM xterm-256color

RUN apt-get update && \
    apt-get install --yes --no-install-recommends --no-install-suggests \
    cmake && \
    rm -rf /var/lib/apt/lists/*

COPY submodules/ tmp/

RUN cd tmp/brotli && \
    mkdir out && cd out && \
    cmake -DBUILD_SHARED_LIBS=0 .. && \
    make && make install
