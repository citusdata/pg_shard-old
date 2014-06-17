# pg_shard/Makefile

MODULES = pg_shard

EXTENSION = pg_shard
DATA = pg_shard--develop.sql

REGRESS = pg_shard

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
