FROM public.ecr.aws/docker/library/postgres:14.7

RUN apt-get update && apt-get install -y postgresql-14-pglogical && rm -rf /var/lib/apt/lists/*

COPY docker-entrypoint-initdb.d /docker-entrypoint-initdb.d
COPY bootstrap.sh /usr/local/bin/
COPY wait_ready.sh /usr/local/bin/

ENV PGDATA /var/lib/postgresql/data-baked
RUN bootstrap.sh
