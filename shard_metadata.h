/*-------------------------------------------------------------------------
 *
 * shard_metadata.h
 *		  Cluster metadata handling for topsie
 *
 * Portions Copyright (c) 2014, Citus Data, Inc.
 *
 * IDENTIFICATION
 *		  shard_metadata.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef TOPSIE_SHARD_METADATA_H
#define TOPSIE_SHARD_METADATA_H

#include "fmgr.h"

#include "nodes/pg_list.h"


/* Configuration for distributing tables resides in this schema. */
#define METADATA_SCHEMA "topsie_metadata"

/* Shard information is stored in the shards table. */
#define SHARDS_TABLE "shards"

/* names for specific attributes within tuples from the shards table */
#define ATTR_NUM_SHARDS_ID 1
#define ATTR_NUM_SHARDS_RELATION_ID 2
#define ATTR_NUM_SHARDS_MIN_VALUE 3
#define ATTR_NUM_SHARDS_MAX_VALUE 4

/* Placement information is stored in the placements table. */
#define PLACEMENTS_TABLE "placements"

/* names for specific attributes within tuples from the placements table */
#define ATTR_NUM_PLACEMENTS_ID 1
#define ATTR_NUM_PLACEMENTS_SHARD_ID 2
#define ATTR_NUM_PLACEMENTS_HOST 3
#define ATTR_NUM_PLACEMENTS_PORT 4


/*
 * TopsieShard represents information about a particular shard in a distributed
 * table. Shards have a unique identifier, a reference back to the foreign table
 * they distribute, and min and max values for the partition column of rows that
 * are contained within the shard (this range is inclusive).
 *
 * All fields are required.
 */
typedef struct TopsieShard
{
	int64 id;			/* unique identifier for the shard */
	Oid relationId;		/* id of the shard's foreign table */
	int32 minValue;		/* a shard's typed min value datum */
	int32 maxValue;		/* a shard's typed max value datum */
} TopsieShard;


/*
 * TopsiePlacement represents information about the placement of a shard in a
 * distributed table. Placements have a unique identifier, a reference to the
 * shard they place, a textual hostname to identify the host on which the shard
 * resides, and a port number to use when connecting to that host.
 *
 * All fields are required.
 */
typedef struct TopsiePlacement
{
	int64 id;		/* unique identifier for the placement */
	int64 shardId;	/* identifies shard for this placement */
	char *host;		/* hostname of machine hosting this shard */
	int32 port;		/* port number for connecting to host */
} TopsiePlacement;


extern List * TopsieLoadShardList(Oid relationId);
extern TopsieShard * TopsieLoadShard(int64 shardId);
extern List * TopsieLoadPlacementList(int64 shardId);
extern Datum topsie_print_metadata(PG_FUNCTION_ARGS);


#endif /* TOPSIE_SHARD_METADATA_H */
