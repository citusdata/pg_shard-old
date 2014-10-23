# Makefile

MODULE_big = pg_shard
OBJS = connection.o create_shards.o distribution_metadata.o extend_ddl_commands.o \
	   generate_ddl_commands.o pg_shard.o prune_shard_list.o ruleutils.o

PG_CPPFLAGS = -std=c99 -Wall -Wextra -I$(libpq_srcdir)

SHLIB_LINK = $(libpq)
EXTENSION = pg_shard
DATA = pg_shard--develop.sql

REGRESS = init connection distribution_metadata

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
