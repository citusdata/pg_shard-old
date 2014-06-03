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
#include "access/htup.h"
#include "access/skey.h"
#include "catalog/namespace.h"
#include "executor/executor.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include "utils/tqual.h"

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
	uint64 id;		/* unique identifier for the placement */
	uint64 shardId;	/* identifies shard for this placement */
	char *host;		/* hostname of machine hosting this shard */
	uint32 port;    /* port number for connecting to host */
} TopsiePlacement;

/* Returns a pointer to a newly palloc'd int64 with the value from src */
static int64 * AllocateInt64(int64 src);

/* Returns a placement populated with values from a given tuple */
static TopsiePlacement * TupleToPlacement(HeapTuple tup, TupleDesc tupleDesc);

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

/*
 * Retrieves the shard metadata for a specified shard identifier. If no such
 * shard exists, an error is thrown.
 */
TopsieShard *
TopsieLoadShard(int64 shardId)
{
	const int scanKeyCount = 1;

	RangeVar *rv = NULL;
	Relation rel = NULL;
	HeapScanDesc scanDesc = NULL;
	ScanKeyData scanKey[scanKeyCount];
	TupleDesc tupleDesc = NULL;
	HeapTuple tup = NULL;
	Datum shardFieldDatum = 0;
	bool shardFieldIsNull = false;
	TopsieShard *shard = NULL;

	shard = (TopsieShard *) palloc0(sizeof(TopsieShard));

	ScanKeyInit(&scanKey[0], ANUM_SHARDS_ID,
			InvalidStrategy, F_INT8EQ, DatumGetInt64(shardId));

	rv = makeRangeVarFromNameList(
			stringToQualifiedNameList(METADATA_SCHEMA "." SHARDS_TABLE));

	rel = relation_openrv(rv, AccessShareLock);
	tupleDesc = RelationGetDescr(rel);
	scanDesc = heap_beginscan(rel, SnapshotNow, scanKeyCount, scanKey);

	if(HeapTupleIsValid(tup = heap_getnext(scanDesc, ForwardScanDirection))) {
		shardFieldDatum = heap_getattr(tup, ANUM_SHARDS_ID, tupleDesc,
				&shardFieldIsNull);
		shard->id = DatumGetInt64(shardFieldDatum);

		shardFieldDatum = heap_getattr(tup, ANUM_SHARDS_RELATION_ID, tupleDesc,
				&shardFieldIsNull);
		shard->relationId = DatumGetObjectId(shardFieldDatum);

		shardFieldDatum = heap_getattr(tup, ANUM_SHARDS_MIN_VALUE, tupleDesc,
				&shardFieldIsNull);
		shard->minValue = DatumGetInt32(shardFieldDatum);

		shardFieldDatum = heap_getattr(tup, ANUM_SHARDS_MAX_VALUE, tupleDesc,
				&shardFieldIsNull);
		shard->maxValue = DatumGetInt32(shardFieldDatum);
	} else {
		ereport(ERROR, (errmsg("could not find entry for shard "
							   INT64_FORMAT, shardId)));
	}

	heap_endscan(scanDesc);
	relation_close(rel, AccessShareLock);

	return shard;
}

/*
 * Return a List of placements related to a given shard.
 */
List *
TopsieLoadPlacementList(int64 shardId)
{
	const int scanKeyCount = 1;

	List *placementList = NIL;
	RangeVar *rv = NULL;
	Relation rel = NULL;
	HeapScanDesc scanDesc = NULL;
	ScanKeyData scanKey[scanKeyCount];
	TupleDesc tupleDesc = NULL;
	HeapTuple tup = NULL;
	TopsiePlacement *placement;
	ScanKeyInit(&scanKey[0], ANUM_PLACEMENTS_SHARD_ID,

			InvalidStrategy, F_INT8EQ, DatumGetInt64(shardId));

	rv = makeRangeVarFromNameList(
			stringToQualifiedNameList(METADATA_SCHEMA "." PLACEMENTS_TABLE));

	rel = relation_openrv(rv, AccessShareLock);

	tupleDesc = RelationGetDescr(rel);
	scanDesc = heap_beginscan(rel, SnapshotNow, scanKeyCount, scanKey);

	while (HeapTupleIsValid(tup = heap_getnext(scanDesc, ForwardScanDirection)))
	{
		placement = TupleToPlacement(tup, tupleDesc);
		lappend(placementList, placement);
	}

	heap_endscan(scanDesc);
	relation_close(rel, AccessShareLock);

	return placementList;
}

static int64 *
AllocateInt64(int64 src)
{
	int64 *dest = (int64 *) palloc0(sizeof(int64));

	*dest = src;

	return dest;
}

static TopsiePlacement *
TupleToPlacement(HeapTuple tup, TupleDesc tupleDesc)
{
	TopsiePlacement *placement = NULL;
	Datum fieldDatum = 0;
	bool fieldIsNull = false;


	placement = palloc0(sizeof(TopsiePlacement));

	fieldDatum = heap_getattr(tup, ANUM_PLACEMENTS_ID, tupleDesc, &fieldIsNull);
	placement->id = DatumGetInt64(fieldDatum);

	fieldDatum = heap_getattr(tup, ANUM_PLACEMENTS_SHARD_ID, tupleDesc,
			&fieldIsNull);
	placement->shardId = DatumGetInt64(fieldDatum);

	fieldDatum = heap_getattr(tup, ANUM_PLACEMENTS_HOST, tupleDesc,
			&fieldIsNull);
	placement->host = TextDatumGetCString(fieldDatum);

	fieldDatum = heap_getattr(tup, ANUM_PLACEMENTS_PORT, tupleDesc,
			&fieldIsNull);
	placement->port = DatumGetInt32(fieldDatum);

	return placement;
}
