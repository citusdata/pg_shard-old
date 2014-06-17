# pg_shard/Makefile

MODULE_big = pg_shard
OBJS = hash.o

EXTENSION = pg_shard
DATA = pg_shard--develop.sql

REGRESS = hash

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
