# Makefile

MODULE_big = pg_shard
OBJS = pg_shard.o connection.o distribution_metadata.o generate_ddl_commands.o \
	extend_ddl_commands.o partition_protocol.o


PG_CPPFLAGS = -std=c99 -pedantic -Wall -Wextra -I$(libpq_srcdir)

SHLIB_LINK = $(libpq)

EXTENSION = pg_shard
DATA = pg_shard--develop.sql

REGRESS = init connection distribution_metadata

PG_CONFIG = /usr/local/pgsql/bin/pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
