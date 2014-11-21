/*-------------------------------------------------------------------------
 *
 * repair_shards.h
 *
 * Functions to implement repair functionality for pg_shard.
 *
 * Copyright (c) 2014, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_SHARD_REPAIR_SHARDS_H
#define PG_SHARD_REPAIR_SHARDS_H

#include "postgres.h"
#include "fmgr.h"


/* Definitions to help with data deletion */
#define DROP_REGULAR_TABLE_COMMAND "DROP TABLE IF EXISTS %s"
#define DROP_FOREIGN_TABLE_COMMAND "DROP FOREIGN TABLE IF EXISTS %s"


/* Function declarations for shard repair functionality */
extern Datum master_copy_shard_placement(PG_FUNCTION_ARGS);


#endif /* PG_SHARD_REPAIR_SHARDS_H */
