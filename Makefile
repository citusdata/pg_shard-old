# Makefile

MODULE_big = pg_shard
OBJS = pg_shard.o distribution_metadata.o

PG_CPPFLAGS = -std=c99 -Weverything -Wno-cast-align \
			  -Wno-missing-field-initializers -Wno-padded -Wno-undef -Wno-vla

EXTENSION = pg_shard
DATA = pg_shard--develop.sql

REGRESS = init distribution_metadata

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
