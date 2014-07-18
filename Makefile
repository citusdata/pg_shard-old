# Makefile

MODULE_big = pg_shard
OBJS = pg_shard.o connection.o distribution_metadata.o ddl_events.o partition_protocol.o

PG_CPPFLAGS = -std=c99 -pedantic -Wall -Wextra \
		-I/usr/local/pgsql/include

SHLIB_LINK = $(libpq)

EXTENSION = pg_shard
DATA = pg_shard--develop.sql

REGRESS = init distribution_metadata

PG_CONFIG = /usr/local/pgsql/bin/pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
