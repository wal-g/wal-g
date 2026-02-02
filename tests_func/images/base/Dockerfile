# vim:set ft=dockerfile:
FROM ubuntu:bionic

ENV DEBIAN_FRONTEND noninteractive
ENV TERM xterm-256color

# Configure apt and locale
RUN apt-get update -q && \
    apt-get install --yes --no-install-recommends --no-install-suggests \
    cmake build-essential wget ca-certificates && \
    rm -rf /var/lib/apt/lists/*

RUN wget -O - "https://go.dev/dl/go1.25.2.linux-amd64.tar.gz" | tar -C /usr/local -xz && \
	export PATH="/usr/local/go/bin:$PATH" && \
	go version

ENV GOPATH /go
ENV PATH $GOPATH/bin:/usr/local/go/bin:$PATH

RUN mkdir -p "$GOPATH/src" "$GOPATH/bin" && chmod -R 777 "$GOPATH"

RUN wget https://github.com/Kitware/CMake/releases/download/v3.31.0/cmake-3.31.0-linux-x86_64.tar.gz && \
    tar xf cmake-3.31.0-linux-x86_64.tar.gz -C /usr --strip-components=1
