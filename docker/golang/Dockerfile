FROM ubuntu:bionic

# custom golang image with CMake and Brotli support (compatible with ubuntu 18.04..22.04)

ENV DEBIAN_FRONTEND=noninteractive
ENV TERM=xterm-256color

# bionic is EOL, archive.ubuntu.com mirrors are flaky; retry & lengthen timeouts
RUN printf 'Acquire::Retries "5";\nAcquire::http::Timeout "30";\nAcquire::https::Timeout "30";\n' \
        > /etc/apt/apt.conf.d/80-retries && \
    apt-get update && \
    apt-get -y install \
        build-essential cmake \
        iputils-ping \
        net-tools dnsutils \
        wget

RUN ARCH="$(uname -m)" && \
    case "$ARCH" in \
        x86_64)  GO_ARCH="amd64" ;; \
        aarch64) GO_ARCH="arm64" ;; \
        *) echo "Unsupported architecture: $ARCH" && exit 1 ;; \
    esac && \
    wget -O - "https://go.dev/dl/go1.25.9.linux-${GO_ARCH}.tar.gz" | tar -C /usr/local -xz && \
    export PATH="/usr/local/go/bin:$PATH" && \
    go version

ENV GOPATH=/go
ENV PATH=$GOPATH/bin:/usr/local/go/bin:$PATH

RUN mkdir -p "$GOPATH/src" "$GOPATH/bin" && chmod -R 777 "$GOPATH"

RUN ARCH="$(uname -m)" && \
    wget "https://github.com/Kitware/CMake/releases/download/v3.31.0/cmake-3.31.0-linux-${ARCH}.tar.gz" && \
    tar xf "cmake-3.31.0-linux-${ARCH}.tar.gz" -C /usr --strip-components=1 && \
    rm "cmake-3.31.0-linux-${ARCH}.tar.gz"

COPY submodules/ tmp/

RUN cd tmp/brotli && \
    mkdir out && cd out && \
    cmake .. && \
    make && make install
