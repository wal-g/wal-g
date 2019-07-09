FROM wal-g/ubuntu:latest

RUN apt-get update -y 
RUN apt-get install mongodb -y

COPY docker/mongo/mongodb.conf /etc/mongodb.conf
