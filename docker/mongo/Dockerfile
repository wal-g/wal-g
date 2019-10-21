FROM wal-g/ubuntu:latest

RUN apt-get update -y 
RUN apt-get install mongodb -y

ENV MONGODBDATA /var/lib/mongodb

COPY docker/mongo/mongodb.conf /etc/mongodb.conf
