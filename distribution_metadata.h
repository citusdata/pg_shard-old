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
#include "postgres_ext.h"

#include "connection.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"


/* schema for configuration related to distributed tables */
#define METADATA_SCHEMA_NAME "pgs_distribution_metadata"

/* table containing shard interval information */
#define SHARD_TABLE_NAME "shard"

/* sequence to obtain new shardId */
#define SHARD_ID_SEQUENCE_NAME "shard_id_sequence"

/* denotes storage type of the underlying shard */
#define SHARD_STORAGE_TABLE 't'
#define SHARD_STORAGE_FOREIGN 'f'

/* table has primary key for fast lookup */
#define SHARD_PKEY_INDEX_NAME "shard_pkey"

/* index to expedite lookup by distributed relation identifier */
#define SHARD_RELATION_INDEX_NAME "shard_relation_index"

/* human-readable names for addressing columns of shard table */
#define SHARD_ATTRIBUTE_COUNT 5
#define ATTR_NUM_SHARD_ID 1
#define ATTR_NUM_SHARD_RELATION_ID 2
#define ATTR_NUM_SHARD_STORAGE 3
#define ATTR_NUM_SHARD_MIN_VALUE 4
#define ATTR_NUM_SHARD_MAX_VALUE 5

/* table containing shard placement information */
#define SHARD_PLACEMENT_TABLE_NAME "shard_placement"

/* sequence to obtain new placement id */
#define SHARD_PLACEMENT_ID_SEQUENCE_NAME "shard_placement_id_sequence"

/* index to expedite lookup by shard identifier */
#define SHARD_PLACEMENT_SHARD_INDEX_NAME "shard_placement_shard_index"

/* human-readable names for addressing columns of shard placement table */
#define SHARD_PLACEMENT_ATTRIBUTE_COUNT 5
#define ATTR_NUM_SHARD_PLACEMENT_ID 1
#define ATTR_NUM_SHARD_PLACEMENT_SHARD_ID 2
#define ATTR_NUM_SHARD_PLACEMENT_SHARD_STATE 3
#define ATTR_NUM_SHARD_PLACEMENT_NODE_NAME 4
#define ATTR_NUM_SHARD_PLACEMENT_NODE_PORT 5

/* table containing information about how to partition distributed tables */
#define PARTITION_TABLE_NAME "partition"

/* human-readable names for addressing columns of partition table */
#define PARTITION_ATTRIBUTE_COUNT 2
#define ATTR_NUM_PARTITION_RELATION_ID 1
#define ATTR_NUM_PARTITION_KEY 2

/* ShardState represents the last known state of a shard on a given node */
typedef enum
{
	STATE_INVALID_FIRST = 0,
	STATE_FINALIZED = 1,
	STATE_CACHED = 2,
	STATE_INACTIVE = 3,
	STATE_TO_DELETE = 4

} ShardState;


/*
 * ShardInterval contains information about a particular shard in a distributed
 * table. ShardIntervals have a unique identifier, a reference back to the table
 * they distribute, and min and max values for the partition column of rows that
 * are contained within the shard (this range is inclusive).
 *
 * All fields are required.
 */
typedef struct ShardInterval
{
	int64 id;			/* unique identifier for the shard */
	Oid relationId;		/* id of the shard's distributed table */
	Datum minValue;		/* a shard's typed min value datum */
	Datum maxValue;		/* a shard's typed max value datum */
	Oid valueTypeId;	/* typeId for minValue and maxValue Datums */
} ShardInterval;


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


/* In-memory representation of a worker node */
typedef struct WorkerNode
{
	uint32 nodePort;
	char nodeName[MAX_NODE_LENGTH];

} WorkerNode;


extern List * LoadShardList(Oid distributedTableId);
extern ShardInterval * LoadShardInterval(int64 shardId);
extern List * LoadShardPlacementList(int64 shardId);
extern Var * PartitionColumn(Oid distributedTableId);
extern Datum TestDistributionMetadata(PG_FUNCTION_ARGS);


#endif /* PG_SHARD_DISTRIBUTION_METADATA_H */
