FROM minio/mc:RELEASE.2019-01-30T19-57-22Z
FROM minio/minio:RELEASE.2019-01-31T00-31-19Z

COPY --from=0 /usr/bin/mc /usr/bin/mc

#ENV MINIO_ACCESS_KEY Eqx7YWt6mDJKV5YuVcGY
#ENV MINIO_SECRET_KEY BssstNFEmXdgc8Y2qheqxNRuGTidjOlkfUqILjKE

ENV MINIO_ACCESS_KEY OcXmmFSxAjd0ncwEZySp
ENV MINIO_SECRET_KEY TOPwCzYieXqmRugkEChMe9qsnzXDZ6SJUg9ZCddi

ENTRYPOINT ["/usr/bin/docker-entrypoint.sh"]

HEALTHCHECK --interval=30s --timeout=5s \
    CMD /usr/bin/healthcheck.sh

EXPOSE 9000

CMD ["server", "/export"]
