FROM minio/mc:RELEASE.2019-01-30T19-57-22Z
FROM minio/minio:RELEASE.2019-01-31T00-31-19Z

COPY --from=0 /usr/bin/mc /usr/bin/mc

ENV MINIO_ACCESS_KEY S3_ACCESS_KEY
ENV MINIO_SECRET_KEY S3_SECRET_KEY

ENTRYPOINT ["/usr/bin/docker-entrypoint.sh"]

HEALTHCHECK --interval=30s --timeout=5s \
    CMD /usr/bin/healthcheck.sh

EXPOSE $S3_PORT

CMD ["server", "/export"]
