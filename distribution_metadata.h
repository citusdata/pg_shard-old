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

#include "postgres.h"
#include "c.h"
#include "fmgr.h"

#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "postgres_ext.h"


/* Configuration for distributing tables resides in this schema. */
#define METADATA_SCHEMA_NAME "pgs_distribution_metadata"

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

/* names for specific attributes in tuples from the partition strategy table */
#define ATTR_NUM_PARTITION_STRATEGY_RELATION_ID 1
#define ATTR_NUM_PARTITION_STRATEGY_KEY 2

/*
 * Shard represents information about a particular shard in a distributed table.
 * Shards have a unique identifier, a reference back to the table they
 * distribute, and min and max values for the partition column of rows that are
 * contained within the shard (this range is inclusive).
 *
 * All fields are required.
 */
typedef struct Shard
{
	int64 id;			/* unique identifier for the shard */
	Oid relationId;		/* id of the shard's distributed table */
	Datum minValue;		/* a shard's typed min value datum */
	Datum maxValue;		/* a shard's typed max value datum */
	Oid valueTypeId;	/* typeId for minValue and maxValue Datums */
} Shard;


/*
 * ShardPlacement represents information about the placement of a shard in a
 * distributed table. ShardPlacements have a unique identifier, a reference to
 * the shard they place, a textual hostname to identify the host on which the
 * shard resides, and a port number to use when connecting to that host.
 *
 * All fields are required.
 */
typedef struct ShardPlacement
{
	int64 id;		/* unique identifier for the shard placement */
	int64 shardId;	/* identifies shard for this shard placement */
	char *nodeName;	/* hostname of machine hosting this shard */
	int32 nodePort;	/* port number for connecting to host */
} ShardPlacement;


extern List * LoadShardList(Oid distributedTableId);
extern Shard * LoadShard(int64 shardId);
extern List * LoadShardPlacementList(int64 shardId);
extern Var * PartitionColumn(Oid distributedTableId);
extern Datum TestDistributionMetadata(PG_FUNCTION_ARGS);


#endif /* PG_SHARD_DISTRIBUTION_METADATA_H */
