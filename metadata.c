/*-------------------------------------------------------------------------
 *
 * metadata.c
 *		  Cluster metadata handling for topsie
 *
 * Portions Copyright (c) 2014, Citus Data, Inc.
 *
 * IDENTIFICATION
 *		  metadata.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "postgres_fdw.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/skey.h"
#include "catalog/namespace.h"
#include "executor/executor.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include "utils/tqual.h"

/* Schema created for topsie metadata */
#define METADATA_SCHEMA "topsie_metadata"

/* Information about shards table */
#define SHARDS_TABLE "shards"

#define ANUM_SHARDS_ID 1
#define ANUM_SHARDS_RELATION_ID 2

/* Information about placements table */
#define PLACEMENTS_TABLE "placements"

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
	uint64 id;		/* unique identifier for the placement */
	uint64 shardId;	/* identifies shard for this placement */
	char *host;		/* hostname of machine hosting this shard */
	uint32 port;    /* port number for connecting to host */
} TopsiePlacement;

/* Returns a pointer to a newly palloc'd int64 with the value from src */
static int64 * AllocateInt64(int64 src);

/*
 * Return a List of shard identifiers related to a given relation.
 */
List *
TopsieLoadShardList(Oid relationId)
{
	const int scanKeyCount = 1;

	List *shardList = NIL;
	RangeVar *rv = NULL;
	Relation rel = NULL;
	HeapScanDesc scanDesc = NULL;
	ScanKeyData scanKey[scanKeyCount];
	TupleDesc tupleDesc = NULL;
	HeapTuple tup = NULL;
	Datum shardIdDatum = 0;
	int64 shardId = 0;
	int64 *shardIdPointer = NULL;
	bool shardIdIsNull = false;

	ScanKeyInit(&scanKey[0], ANUM_SHARDS_RELATION_ID,
			InvalidStrategy, F_OIDEQ, ObjectIdGetDatum(relationId));

	rv = makeRangeVarFromNameList(
			stringToQualifiedNameList(METADATA_SCHEMA "." SHARDS_TABLE));

	rel = relation_openrv(rv, AccessShareLock);

	tupleDesc = RelationGetDescr(rel);
	scanDesc = heap_beginscan(rel, SnapshotNow, scanKeyCount, scanKey);

	while (HeapTupleIsValid(tup = heap_getnext(scanDesc, ForwardScanDirection)))
	{
		shardIdDatum = heap_getattr(tup, ANUM_SHARDS_ID, tupleDesc, &shardIdIsNull);
		shardId = DatumGetInt64(shardIdDatum);
		shardIdPointer = AllocateInt64(shardId);

		shardList = lappend(shardList, shardIdPointer);
	}

	heap_endscan(scanDesc);
	relation_close(rel, AccessShareLock);

	return shardList;
}

static int64 *
AllocateInt64(int64 src)
{
	int64 *dest = (int64 *) palloc0(sizeof(int64));

	*dest = src;

	return dest;
}
