MODULES = lwaldump

EXTENSION = lwaldump
DATA = lwaldump--1.0.sql
PGFILEDESC = "lwaldump - allows get last flushed to WAL lsn even if PostgreSQL was crushed"

REGRESS = lwaldump

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
