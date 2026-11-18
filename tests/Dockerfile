ARG PG_TAG
FROM postgres:${PG_TAG}

ARG PG_MAJOR
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential libipc-run-perl perl postgresql-server-dev-${PG_MAJOR} \
    && rm -rf /var/lib/apt/lists/*

ENV PG_CONFIG=/usr/lib/postgresql/${PG_MAJOR}/bin/pg_config
ENV PATH=/usr/lib/postgresql/${PG_MAJOR}/bin:${PATH}

COPY . /src/lwaldump
RUN make -C /src/lwaldump clean \
    && make -C /src/lwaldump \
    && make -C /src/lwaldump install \
    && grep -Eq '^enable_tap_tests[[:space:]]*=[[:space:]]*yes([[:space:]]|$)' \
        "$(dirname "$(${PG_CONFIG} --pgxs)")/../Makefile.global" \
    && chown -R postgres:postgres /src/lwaldump

USER postgres
WORKDIR /src/lwaldump
ENTRYPOINT ["make", "installcheck"]
