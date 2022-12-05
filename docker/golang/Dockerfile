FROM golang:alpine

ENV TERM xterm-256color

RUN apk update && \
    apk add musl-dev bash git gcc make cmake curl upx 
RUN git config --global --add safe.directory /app
WORKDIR /app

COPY submodules/ tmp/

RUN cd tmp/brotli && \
    mkdir out && cd out && \
    ../configure-cmake --disable-debug && \
    make && make install
