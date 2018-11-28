FROM golang:latest

ENV DEBIAN_FRONTEND noninteractive
ENV TERM xterm-256color

RUN apt-get update && \
    apt-get install --yes \
    cmake && \
    rm -rf /var/lib/apt/lists/*

COPY submodules/ tmp/

RUN cd tmp/brotli && \
    mkdir out && cd out && \
    ../configure-cmake --disable-debug && \
    make && make install
