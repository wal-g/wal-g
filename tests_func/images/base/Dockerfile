# vim:set ft=dockerfile:
FROM ubuntu:bionic

ENV DEBIAN_FRONTEND noninteractive
ENV LANG en_US.utf8

# Configure apt and locale
COPY ./config/apt/ /var/lib/dist/config/apt
RUN mkdir -p /code /config /data/db /tmp/storage

RUN apt-get clean && apt-get update && apt-get install -y locales

RUN cp --force --recursive --verbose \
    /var/lib/dist/config/apt/* /etc/apt/ && \
    echo 'en_US.UTF-8 UTF-8' > /etc/locale.gen && \
    locale-gen && \
    apt-get -qq update && \
    \
    apt-get -y install \
    supervisor wget curl less vim ca-certificates \
    openssl git openssh-client openssh-server \
    build-essential cmake \
    netcat-openbsd iputils-ping \
    net-tools dnsutils gnupg \
    nginx nginx-full nginx-common \
	&& rm -rf /var/lib/apt/lists/*

RUN wget -O - "https://golang.org/dl/go1.18.linux-amd64.tar.gz" | tar -C /usr/local -xz && \
	export PATH="/usr/local/go/bin:$PATH" && \
	go version

ENV GOPATH /go
ENV PATH $GOPATH/bin:/usr/local/go/bin:$PATH

RUN mkdir -p "$GOPATH/src" "$GOPATH/bin" && chmod -R 777 "$GOPATH"

RUN echo "root:root" | chpasswd && \
    sed -i 's/#PermitRootLogin .*/PermitRootLogin yes/g' /etc/ssh/sshd_config && \
    mkdir /var/run/sshd

COPY ./config/supervisor /etc/supervisor

CMD ["supervisord", "-c", "/etc/supervisor/supervisord.conf"]
