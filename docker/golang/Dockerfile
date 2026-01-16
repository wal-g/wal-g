FROM ubuntu:bionic

# custom golang image with CMake and Brotli support (compatible with ubuntu 18.04..22.04)

ENV DEBIAN_FRONTEND noninteractive
ENV TERM xterm-256color

RUN apt-get update && \
    apt-get -y install \
        build-essential cmake \
        iputils-ping \
        net-tools dnsutils \
        wget

# Detect architecture and download appropriate Go version
RUN ARCH=$(dpkg --print-architecture) && \
    if [ "$ARCH" = "amd64" ]; then \
        GO_ARCH="amd64"; \
    elif [ "$ARCH" = "arm64" ]; then \
        GO_ARCH="arm64"; \
    else \
        echo "Unsupported architecture: $ARCH" && exit 1; \
    fi && \
    wget -O - "https://go.dev/dl/go1.25.2.linux-${GO_ARCH}.tar.gz" | tar -C /usr/local -xz && \
    export PATH="/usr/local/go/bin:$PATH" && \
    go version

ENV GOPATH /go
ENV PATH $GOPATH/bin:/usr/local/go/bin:$PATH

RUN mkdir -p "$GOPATH/src" "$GOPATH/bin" && chmod -R 777 "$GOPATH"

# Detect architecture and download appropriate CMake version
RUN ARCH=$(dpkg --print-architecture) && \
    if [ "$ARCH" = "amd64" ]; then \
        CMAKE_ARCH="x86_64"; \
    elif [ "$ARCH" = "arm64" ]; then \
        CMAKE_ARCH="aarch64"; \
    else \
        echo "Unsupported architecture: $ARCH" && exit 1; \
    fi && \
    wget https://github.com/Kitware/CMake/releases/download/v3.31.0/cmake-3.31.0-linux-${CMAKE_ARCH}.tar.gz && \
    tar xf cmake-3.31.0-linux-${CMAKE_ARCH}.tar.gz -C /usr --strip-components=1

COPY submodules/ tmp/

RUN cd tmp/brotli && \
    mkdir out && cd out && \
    cmake .. && \
    make && make install
