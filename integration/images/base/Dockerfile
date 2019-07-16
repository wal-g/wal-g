# vim:set ft=dockerfile:
FROM registry.yandex.net/ubuntu:xenial

ENV DEBIAN_FRONTEND noninteractive
ENV LANG en_US.utf8
ENV WALG_FILE_PREFIX=/tmp/backups

# Configure apt and locale
COPY ./config/apt/ /var/lib/dist/config/apt
#COPY ./config/common_walg.env /tmp/common_walg.env

RUN cp --force --recursive --verbose \
    /var/lib/dist/config/apt/* /etc/apt/ && \
    echo 'en_US.UTF-8 UTF-8' > /etc/locale.gen && \
    locale-gen && \
    apt-get -qq update && \
    \
    apt-get -y install \
    supervisor=3.3.1-1yandex \
    openssl git openssh-client openssh-server \
    python3-pip \
    python3-setuptools \
    python3-dev \
    build-essential \
    netcat-openbsd \
    iputils-ping \
    net-tools \
    dnsutils \
    nginx=1.8.1-1.yandex.16 \
    nginx-full=1.8.1-1.yandex.16 \
    nginx-common=1.8.1-1.yandex.16

RUN mkdir -p /opt/yandex /code /config
RUN mkdir /tmp/backups
COPY ./config/.wal-g.yaml /root/.wal-g.yaml 

RUN pip3 install --upgrade pip

RUN echo "root:root" | chpasswd
RUN sed -i -e '/PermitRootLogin/ s/ .*/ yes/' /etc/ssh/sshd_config
RUN mkdir /var/run/sshd

COPY ./config/supervisor /etc/supervisor

CMD ["supervisord", "-c", "/etc/supervisor/supervisord.conf"]

