ARG postgres_version
FROM postgres:${postgres_version}-alpine

COPY docker-entrypoint-initdb.d /docker-entrypoint-initdb.d
COPY bootstrap.sh /usr/local/bin/
COPY wait_ready.sh /usr/local/bin/

ENV PGDATA /var/lib/postgresql/data-baked
RUN bootstrap.sh
