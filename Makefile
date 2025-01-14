# contrib/lwaldump/Makefile

MODULE_big	= lwaldump
OBJS = \
	$(WIN32RES) \
	lwaldump.o

EXTENSION = lwaldump
DATA = lwaldump--1.0.sql

REGRESS = check

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/lwaldump
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
