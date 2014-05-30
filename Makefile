# Makefile

MODULE_big = topsie
OBJS = postgres_fdw.o option.o deparse.o connection.o

PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK = $(libpq)

EXTENSION = topsie
DATA = topsie--develop.sql

REGRESS = topsie

# the db name is hard-coded in the tests
override USE_MODULE_DB =

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
