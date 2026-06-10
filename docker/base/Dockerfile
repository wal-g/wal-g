FROM debian:bullseye-slim

ENV DEBIAN_FRONTEND=noninteractive
ENV TERM=xterm-256color

# apt hygiene: no recommends/suggests, assume-yes (shared base for engine images)
COPY docker/ubuntu/config/apt/ /var/lib/dist/config/apt
RUN cp --force --recursive /var/lib/dist/config/apt/* /etc/apt/

# git+ca-certificates for source fetches, liblzo2-2 runtime lib, wget+curl for installers
RUN apt-get update && \
    apt-get install --yes --no-install-recommends --no-install-suggests \
    git ca-certificates liblzo2-2 wget curl && \
    rm -rf /var/lib/apt/lists/*
