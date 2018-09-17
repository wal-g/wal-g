FROM ubuntu
RUN apt update
RUN yes | apt install liblzo2-2
COPY pginstall.sh /tmp
RUN chmod +x /tmp/pginstall.sh
RUN /tmp/pginstall.sh | apt install postgresql-10 postgresql-contrib
ENV PGDATA=/var/lib/postgresql/10/main
RUN rm -rf $PGDATA
COPY wal-g /usr/bin
COPY run_integration_tests.sh /tmp
COPY full_backup_test.sh /tmp
COPY delta_backup_fullscan_test.sh /tmp
COPY delta_backup_wal_delta_test.sh /tmp
CMD su postgres -c "/tmp/run_integration_tests.sh"
