MODULES = lw_waldump

EXTENSION = lw_waldump
DATA = lw_waldump--1.0.sql
PGFILEDESC = "lw_waldump - allows get last flushed to WAL lsn even if PostgreSQL was crushed"

REGRESS = lw_waldump

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
