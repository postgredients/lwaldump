ARG PG_TAG
FROM postgres:${PG_TAG}

ARG PG_MAJOR
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential postgresql-server-dev-${PG_MAJOR} \
    && rm -rf /var/lib/apt/lists/*

COPY . /src/lwaldump
RUN make -C /src/lwaldump clean \
    && make -C /src/lwaldump PG_CONFIG=/usr/lib/postgresql/${PG_MAJOR}/bin/pg_config \
    && make -C /src/lwaldump PG_CONFIG=/usr/lib/postgresql/${PG_MAJOR}/bin/pg_config install

COPY tests/matrix-test.sh /usr/local/bin/matrix-test.sh
RUN chmod 755 /usr/local/bin/matrix-test.sh
ENTRYPOINT ["/usr/local/bin/matrix-test.sh"]
