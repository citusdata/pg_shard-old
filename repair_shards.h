/*-------------------------------------------------------------------------
 *
 * repair_shards.h
 *			Repair functionality for pg_shard.
 *
 * Portions Copyright (c) 2014, Citus Data, Inc.
 *
 * IDENTIFICATION
 *			repair_shards.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_SHARD_REPAIR_SHARDS_H
#define PG_SHARD_REPAIR_SHARDS_H

#include "postgres.h"
#include "fmgr.h"


extern Datum repair_shard(PG_FUNCTION_ARGS);

#endif /* PG_SHARD_H */
