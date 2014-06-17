# Makefile

MODULE_big = pg_shard
OBJS = hash.o

PG_CPPFLAGS = -std=c99 -Weverything -Wno-cast-align \
			  -Wno-missing-field-initializers -Wno-padded -Wno-undef

EXTENSION = pg_shard
DATA = pg_shard--develop.sql

REGRESS = hash

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
