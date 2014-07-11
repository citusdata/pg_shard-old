# Makefile

MODULE_big = pg_shard
OBJS = pg_shard.o distribution_metadata.o

PG_CPPFLAGS = -std=c99 -pedantic -fstack-protector \
	-Wall \
	-Wextra \
	-Waggregate-return \
	-Wbad-function-cast \
	-Wcast-qual \
	-Wconversion \
	-Wdisabled-optimization \
	-Wfloat-equal \
	-Winit-self \
	-Winline \
	-Wmissing-declarations \
	-Wmissing-format-attribute \
	-Wmissing-prototypes \
	-Wnested-externs \
	-Wold-style-definition \
	-Wshadow \
	-Wstrict-overflow \
	-Wstrict-prototypes \
	-Wswitch-default

EXTENSION = pg_shard
DATA = pg_shard--develop.sql

REGRESS = init distribution_metadata

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
