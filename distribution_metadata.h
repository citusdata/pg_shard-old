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


/* Configuration for distributing tables resides in this schema. */
#define METADATA_SCHEMA "pgs_metadata"

/* Shard information is stored in the shard table. */
#define SHARD_TABLE "shard"

/* names for specific attributes within tuples from the shard table */
#define ATTR_NUM_SHARD_ID 1
#define ATTR_NUM_SHARD_RELATION_ID 2
#define ATTR_NUM_SHARD_MIN_VALUE 3
#define ATTR_NUM_SHARD_MAX_VALUE 4

/* Placement information is stored in the placement table. */
#define PLACEMENT_TABLE "placement"

/* names for specific attributes within tuples from the placement table */
#define ATTR_NUM_PLACEMENT_ID 1
#define ATTR_NUM_PLACEMENT_SHARD_ID 2
#define ATTR_NUM_PLACEMENT_HOST 3
#define ATTR_NUM_PLACEMENT_PORT 4


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
	char *host;		// hostname of machine hosting this shard
	int32 port;		// port number for connecting to host
} PgsPlacement;


extern List * PgsLoadShardList(Oid relationId);
extern PgsShard * PgsLoadShard(int64 shardId);
extern List * PgsLoadPlacementList(int64 shardId);
extern Datum PgsPrintMetadata(PG_FUNCTION_ARGS);


#endif /* PG_SHARD_DISTRIBUTION_METADATA_H */
