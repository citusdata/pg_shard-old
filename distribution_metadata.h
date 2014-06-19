/*-------------------------------------------------------------------------
 *
 * distribution_metadata.h
 *		  Cluster metadata handling for pg_shard
 *
 * Portions Copyright (c) 2014, Citus Data, Inc.
 *
 * IDENTIFICATION
 *		  distribution_metadata.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_SHARD_DISTRIBUTION_METADATA_H
#define PG_SHARD_DISTRIBUTION_METADATA_H

#include "fmgr.h"

#include "nodes/pg_list.h"
#include "nodes/primnodes.h"


/* Configuration for distributing tables resides in this schema. */
#define METADATA_SCHEMA "pgs_metadata"

/* Shard information is stored in the shard table. */
#define SHARD_TABLE_NAME "shard"

/* The table has a primary key index for fast lookup. */
#define SHARD_PKEY_IDX "shard_pkey"

/* The table has an index to expedite lookup by relation identifier */
#define SHARD_RELATION_IDX "shard_idx_relation_id"

/* names for specific attributes within tuples from the shard table */
#define ATTR_NUM_SHARD_ID 1
#define ATTR_NUM_SHARD_RELATION_ID 2
#define ATTR_NUM_SHARD_MIN_VALUE 3
#define ATTR_NUM_SHARD_MAX_VALUE 4

/* Placement information is stored in the placement table. */
#define PLACEMENT_TABLE_NAME "placement"

/* The table has an index to expedite lookup by shard identifier */
#define PLACEMENT_SHARD_IDX "placement_idx_shard_id"

/* names for specific attributes within tuples from the placement table */
#define ATTR_NUM_PLACEMENT_ID 1
#define ATTR_NUM_PLACEMENT_SHARD_ID 2
#define ATTR_NUM_PLACEMENT_NODE_NAME 3
#define ATTR_NUM_PLACEMENT_NODE_PORT 4

/*
 * Partition strategies are stored in the partition_strategy table, one for each
 * distributed table tracked by pg_shard.
 */
#define PARTITION_STRATEGY_TABLE_NAME "partition_strategy"

/* The table has an index to expedite lookup by relation identifier */
#define PARTITION_STRATEGY_RELATION_IDX "partition_strategy_relation_id_key"

/* names for specific attributes in tuples from the partition strategy table */
#define ATTR_NUM_PARTITION_STRATEGY_RELATION_ID 1
#define ATTR_NUM_PARTITION_STRATEGY_KEY 2

/*
 * PgsShard represents information about a particular shard in a distributed
 * table. Shards have a unique identifier, a reference back to the foreign table
 * they distribute, and min and max values for the partition column of rows that
 * are contained within the shard (this range is inclusive).
 *
 * All fields are required.
 */
typedef struct PgsShard
{
	int64 id;		// unique identifier for the shard
	Oid relationId;	// id of the shard's foreign table
	int32 minValue;	// a shard's typed min value datum
	int32 maxValue;	// a shard's typed max value datum
} PgsShard;


/*
 * PgsPlacement represents information about the placement of a shard in a
 * distributed table. Placements have a unique identifier, a reference to the
 * shard they place, a textual hostname to identify the host on which the shard
 * resides, and a port number to use when connecting to that host.
 *
 * All fields are required.
 */
typedef struct PgsPlacement
{
	int64 id;		// unique identifier for the placement
	int64 shardId;	// identifies shard for this placement
	char *nodeName;	// hostname of machine hosting this shard
	int32 nodePort;	// port number for connecting to host
} PgsPlacement;


extern List * PgsLoadShardList(Oid relationId);
extern PgsShard * PgsLoadShard(int64 shardId);
extern List * PgsLoadPlacementList(int64 shardId);
extern Var * PgsPartitionColumn(Oid relationId);
extern Datum PgsPrintMetadata(PG_FUNCTION_ARGS);


#endif /* PG_SHARD_DISTRIBUTION_METADATA_H */
