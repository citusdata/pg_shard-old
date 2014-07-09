# Makefile

MODULE_big = pg_shard
OBJS = pg_shard.o distribution_metadata.o

PG_CPPFLAGS = -std=c99 -pedantic -Wall -Wextra -fstack-protector \
	-Waggregate-return \
	-Wbad-function-cast \
	-Wcast-qual \
	-Wconversion \
	-Wdeclaration-after-statement \
	-Wdisabled-optimization \
	-Wfloat-equal \
	-Winit-self \
	-Winline \
	-Wmissing-declarations \
	-Wmissing-format-attribute \
	-Wmissing-prototypes \
	-Wnested-externs \
	-Wold-style-definition \
	-Wredundant-decls \
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
