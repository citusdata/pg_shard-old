# contrib/topsie/Makefile

MODULES = topsie

EXTENSION = topsie
DATA = topsie--develop.sql

REGRESS = topsie

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
