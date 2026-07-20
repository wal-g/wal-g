FROM ubuntu:18.04

ENV DEBIAN_FRONTEND=noninteractive
ENV TERM=xterm-256color

# bionic is EOL, archive.ubuntu.com mirrors are flaky; retry & lengthen timeouts
RUN printf 'Acquire::Retries "5";\nAcquire::http::Timeout "30";\nAcquire::https::Timeout "30";\n' \
        > /etc/apt/apt.conf.d/80-retries && \
    apt-get update && \
    apt-get install --yes --no-install-recommends --no-install-suggests \
    git ca-certificates \
    liblzo2-2 && \
    rm -rf /var/lib/apt/lists/* /var/cache/debconf/*