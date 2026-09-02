# contrib/lwaldump/Makefile

MODULE_big	= lwaldump
OBJS = \
	$(WIN32RES) \
	lwaldump.o

EXTENSION = lwaldump
DATA = lwaldump--1.0.sql

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
