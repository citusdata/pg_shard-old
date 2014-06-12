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


/* Schema created for topsie metadata */
#define METADATA_SCHEMA "topsie_metadata"

/* Information about shards table */
#define SHARDS_TABLE "shards"

#define ANUM_SHARDS_ID 1
#define ANUM_SHARDS_RELATION_ID 2
#define ANUM_SHARDS_MIN_VALUE 3
#define ANUM_SHARDS_MAX_VALUE 4

/* Information about placements table */
#define PLACEMENTS_TABLE "placements"

#define ANUM_PLACEMENTS_ID 1
#define ANUM_PLACEMENTS_SHARD_ID 2
#define ANUM_PLACEMENTS_HOST 3
#define ANUM_PLACEMENTS_PORT 4


/* In-memory representation of a tuple from topsie.shards */
typedef struct TopsieShard
{
	int64 id;			/* unique identifier for the shard */
	Oid relationId;		/* id of the shard's foreign table */
	int32 minValue;		/* a shard's typed min value datum */
	int32 maxValue;		/* a shard's typed max value datum */
} TopsieShard;


/* In-memory representation of a tuple from topsie.placements */
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
