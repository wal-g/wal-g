FROM ubuntu
RUN apt update
RUN yes | apt install liblzo2-2
COPY pginstall.sh /tmp
RUN chmod +x /tmp/pginstall.sh
RUN /tmp/pginstall.sh | apt install postgresql-10 postgresql-contrib
ENV PGDATA=/var/lib/postgresql/10/main
RUN rm -rf $PGDATA
RUN su postgres -c "/usr/lib/postgresql/10/bin/initdb $PGDATA"
COPY wal-g /usr/bin
COPY initdb.sh /tmp
RUN chmod +x /tmp/initdb.sh
RUN echo "archive_mode = on" >> /var/lib/postgresql/10/main/postgresql.conf
RUN echo "archive_command = '/usr/bin/timeout 600 /usr/bin/wal-g wal-push %p'" >> /var/lib/postgresql/10/main/postgresql.conf
RUN echo "archive_timeout = 600" >> /var/lib/postgresql/10/main/postgresql.conf
CMD su postgres -c "/tmp/initdb.sh"
