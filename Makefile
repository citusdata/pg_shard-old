EXTENSION = topsie

DATA = topsie--develop.sql

REGRESS = topsie
REGRESS_OPTS = --extra-install=contrib/postgres_fdw

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
