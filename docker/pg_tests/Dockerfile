FROM wal-g/docker_prefix:latest

COPY docker/pg_tests/scripts/ /

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y netcat-openbsd && apt-get clean

CMD su postgres -c "/tmp/run_integration_tests.sh"