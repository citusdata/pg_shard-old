/*-------------------------------------------------------------------------
 *
 * shard_metadata.c
 *		  Cluster metadata handling for topsie
 *
 * Portions Copyright (c) 2014, Citus Data, Inc.
 *
 * IDENTIFICATION
 *		  shard_metadata.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "shard_metadata.h"

#include "fmgr.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/htup.h"
#include "access/skey.h"
#include "catalog/namespace.h"
#include "executor/executor.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/palloc.h"
#include "utils/rel.h"
#include "utils/tqual.h"

/* Returns a pointer to a newly palloc'd int64 with the value from src */
static int64 * AllocateInt64(int64 src);

/* Returns a placement populated with values from a given tuple */
static TopsiePlacement * TupleToPlacement(HeapTuple tup, TupleDesc tupleDesc);

PG_FUNCTION_INFO_V1(topsie_print_metadata);

/*
 * Walks over all shard/placement configuration and prints it at INFO level for
 * testing purposes.
 *
 * FIXME: Remove before release
 */
Datum
topsie_print_metadata(PG_FUNCTION_ARGS)
{
	Oid relationId = InvalidOid;

	List *shardList = NIL;
	TopsieShard *shard = NULL;
	List *placementList = NIL;

	ListCell   *cell = NULL;

	relationId = PG_GETARG_OID(0);

	shardList = TopsieLoadShardList(relationId);
	ereport(INFO, (errmsg("Found %d shards...", list_length(shardList))));

	foreach(cell, shardList)
	{
		ListCell *placementCell = NULL;
		int64 *shardId = NULL;

		shardId = (int64 *) lfirst(cell);
		shard = TopsieLoadShard(*shardId);

		ereport(INFO, (errmsg("Shard #" INT64_FORMAT, shard->id)));
		ereport(INFO,
				(errmsg("\trelation:\t%s", get_rel_name(shard->relationId))));
		ereport(INFO, (errmsg("\tmin value:\t%d", shard->minValue)));
		ereport(INFO, (errmsg("\tmax value:\t%d", shard->maxValue)));

		placementList = TopsieLoadPlacementList(*shardId);
		ereport(INFO, (errmsg("\t%d placements:", list_length(placementList))));

		foreach(placementCell, placementList)

		{
			TopsiePlacement *placement = NULL;

			placement = (TopsiePlacement *) lfirst(placementCell);

			ereport(INFO,
					(errmsg("\t\tPlacement #" INT64_FORMAT, placement->id)));
			ereport(INFO,
					(errmsg("\t\t\tshard:\t" INT64_FORMAT, placement->shardId)));
			ereport(INFO, (errmsg("\t\t\thost:\t%s", placement->host)));
			ereport(INFO, (errmsg("\t\t\tport:\t%u", placement->port)));
		}
	}

	PG_RETURN_VOID();
}

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
	bool isNull = false;

	rv = makeRangeVarFromNameList(
			stringToQualifiedNameList(METADATA_SCHEMA "." SHARDS_TABLE));

	rel = relation_openrv(rv, AccessShareLock);

	tupleDesc = RelationGetDescr(rel);

	ScanKeyInit(&scanKey[0], ANUM_SHARDS_RELATION_ID,
			InvalidStrategy, F_OIDEQ, ObjectIdGetDatum(relationId));

	scanDesc = heap_beginscan(rel, SnapshotNow, scanKeyCount, scanKey);

	while (HeapTupleIsValid(tup = heap_getnext(scanDesc, ForwardScanDirection)))
	{
		Datum shardIdDatum = heap_getattr(tup, ANUM_SHARDS_ID, tupleDesc, &isNull);

		int64 shardId = DatumGetInt64(shardIdDatum);
		int64 *shardIdPointer = AllocateInt64(shardId);

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
			InvalidStrategy, F_INT8EQ, Int64GetDatum(shardId));

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

	rv = makeRangeVarFromNameList(
			stringToQualifiedNameList(METADATA_SCHEMA "." PLACEMENTS_TABLE));

	rel = relation_openrv(rv, AccessShareLock);

	tupleDesc = RelationGetDescr(rel);

	ScanKeyInit(&scanKey[0], ANUM_PLACEMENTS_SHARD_ID,
			InvalidStrategy, F_INT8EQ, Int64GetDatum(shardId));

	scanDesc = heap_beginscan(rel, SnapshotNow, scanKeyCount, scanKey);

	while (HeapTupleIsValid(tup = heap_getnext(scanDesc, ForwardScanDirection)))
	{
		TopsiePlacement *placement = TupleToPlacement(tup, tupleDesc);
		placementList = lappend(placementList, placement);
	}

	heap_endscan(scanDesc);
	relation_close(rel, AccessShareLock);

	/* if no shard placements are found, warn the user */
	if (placementList == NIL)
	{
		ereport(WARNING, (errmsg("could not find any placements for shardId "
								 INT64_FORMAT, shardId)));
	}

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
	bool isNull = false;

	Datum idDatum = heap_getattr(tup, ANUM_PLACEMENTS_ID, tupleDesc, &isNull);
	Datum shardIdDatum = heap_getattr(tup, ANUM_PLACEMENTS_SHARD_ID, tupleDesc,
			&isNull);
	Datum hostDatum = heap_getattr(tup, ANUM_PLACEMENTS_HOST, tupleDesc,
			&isNull);
	Datum portDatm = heap_getattr(tup, ANUM_PLACEMENTS_PORT, tupleDesc,
			&isNull);

	Assert(!HeapTupleHasNulls(tup));

	placement = palloc0(sizeof(TopsiePlacement));
	placement->id = DatumGetInt64(idDatum);
	placement->shardId = DatumGetInt64(shardIdDatum);
	placement->host = TextDatumGetCString(hostDatum);
	placement->port = DatumGetInt32(portDatm);

	return placement;
}
