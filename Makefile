# Makefile

MODULE_big = pg_shard
OBJS = pg_shard.o distribution_metadata.o

PG_CPPFLAGS = -std=c99 -pedantic -Wall -Wextra

EXTENSION = pg_shard
DATA = pg_shard--develop.sql

REGRESS = init distribution_metadata

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
