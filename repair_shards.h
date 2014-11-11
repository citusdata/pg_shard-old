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


#define DROP_REGULAR_TABLE_COMMAND "DROP TABLE IF EXISTS %s"
#define DROP_FOREIGN_TABLE_COMMAND "DROP FOREIGN TABLE IF EXISTS %s"
#define SELECT_ALL_QUERY "SELECT * FROM %s"
#define COPY_RELATION_QUERY "SELECT copy_relation_from_node(%s, %s, %d)"


extern Datum master_copy_shard_placement(PG_FUNCTION_ARGS);
Datum copy_relation_from_node(PG_FUNCTION_ARGS);


#endif /* PG_SHARD_REPAIR_SHARDS_H */
