FROM wal-g/docker_prefix:latest

COPY docker/pg_tests/scripts/ /

CMD /tmp/scripts/start_ssh.sh ; su postgres -c "/tmp/run_integration_tests.sh"