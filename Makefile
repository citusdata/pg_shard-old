# pg_shard/Makefile
#
# Copyright (c) 2014 Citus Data, Inc.
#

MODULE_big = pg_shard
OBJS = connection.o create_shards.o distribution_metadata.o extend_ddl_commands.o \
	   generate_ddl_commands.o pg_shard.o prune_shard_list.o ruleutils.o

PG_CPPFLAGS = -std=c99 -Wall -Wextra -I$(libpq_srcdir)

# pg_shard and CitusDB have several functions that share the same name. When we
# link pg_shard against CitusDB on Linux, the loader resolves to the CitusDB
# function first. We override that behavior and make sure the loader calls the
# pg_shard function instead.
#
OS := $(shell uname)
ifeq ($(OS), Linux)
     SHLIB_LINK = $(libpq) -Wl,-Bsymbolic
else
     SHLIB_LINK = $(libpq)
endif

EXTENSION = pg_shard
DATA = pg_shard--develop.sql

REGRESS = init connection distribution_metadata

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
