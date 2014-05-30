# Makefile

MODULE_big = postgres_fdw
OBJS = postgres_fdw.o option.o deparse.o connection.o

PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK = $(libpq)

EXTENSION = postgres_fdw
DATA = postgres_fdw--develop.sql

REGRESS = postgres_fdw

# the db name is hard-coded in the tests
override USE_MODULE_DB =

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
