# vim:set ft=dockerfile:
FROM ubuntu:xenial

ENV DEBIAN_FRONTEND noninteractive
ENV WALG_FILE_PREFIX /tmp/backups
ENV LANG en_US.utf8 

# Configure apt and locale
COPY ./config/apt/ /var/lib/dist/config/apt
COPY ./config/.wal-g.yaml $HOME/.wal-g.yaml

RUN apt-get clean && apt-get update && apt-get install -y locales

RUN cp --force --recursive --verbose \
    /var/lib/dist/config/apt/* /etc/apt/ && \
    echo 'en_US.UTF-8 UTF-8' > /etc/locale.gen && \
    locale-gen && \
    apt-get -qq update && \
    \
    apt-get -y install \
    supervisor \
    openssl git openssh-client openssh-server \
    build-essential \
    netcat-openbsd \
    iputils-ping \
    net-tools \
    dnsutils \
    nginx \
    nginx-full \
    nginx-common

RUN echo "root:root" | chpasswd
RUN sed -i -e '/PermitRootLogin/ s/ .*/ yes/' /etc/ssh/sshd_config
RUN mkdir /var/run/sshd

RUN mkdir -p /code /config /data/db
RUN mkdir /tmp/backups

COPY ./config/supervisor /etc/supervisor

CMD ["supervisord", "-c", "/etc/supervisor/supervisord.conf"]
