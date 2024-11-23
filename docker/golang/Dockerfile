FROM debian:buster

# Note: we used `golang:buster` here... however it provides go 1.20.x, while we upgraded to 1.22+
# Migration to `golang:bullseye` doesn't work - it incopatible with ubuntu 18.04 (GLIBC version issues).
# So, install it manually:

ENV DEBIAN_FRONTEND noninteractive
ENV TERM xterm-256color

RUN apt-get update && \
    apt-get -y install \
        build-essential cmake \
        iputils-ping \
        net-tools dnsutils \
        wget

RUN wget -O - "https://go.dev/dl/go1.22.9.linux-amd64.tar.gz" | tar -C /usr/local -xz && \
	export PATH="/usr/local/go/bin:$PATH" && \
	go version

ENV GOPATH /go
ENV PATH $GOPATH/bin:/usr/local/go/bin:$PATH

RUN mkdir -p "$GOPATH/src" "$GOPATH/bin" && chmod -R 777 "$GOPATH"

RUN wget https://github.com/Kitware/CMake/releases/download/v3.31.0/cmake-3.31.0-linux-x86_64.tar.gz && \
    tar xf cmake-3.31.0-linux-x86_64.tar.gz -C /usr --strip-components=1

COPY submodules/ tmp/

RUN cd tmp/brotli && \
    mkdir out && cd out && \
    cmake .. && \
    make && make install
