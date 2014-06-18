/*-------------------------------------------------------------------------
 *
 * distribution_metadata.c
 *		  Cluster metadata handling for pg_shard
 *
 * Portions Copyright (c) 2014, Citus Data, Inc.
 *
 * IDENTIFICATION
 *		  shard_metadata.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"

#include "distribution_metadata.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/htup.h"
#include "access/skey.h"
#include "executor/executor.h"
#include "nodes/makefuncs.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/palloc.h"
#include "utils/rel.h"
#include "utils/tqual.h"


/* local function forward declarations */
static PgsShard * TupleToShard(HeapTuple tup, TupleDesc tupleDesc);
static PgsPlacement * TupleToPlacement(HeapTuple tup, TupleDesc tupleDesc);


/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(PgsPrintMetadata);


/*
 * pgs_print_metadata prints all current shard and placement configuration
 * at INFO level for testing purposes.
 *
 * FIXME: Remove before release
 */
Datum
PgsPrintMetadata(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);

	List *shardList = PgsLoadShardList(relationId);
	List *placementList = NIL;

	ListCell *cell = NULL;

	ereport(INFO, (errmsg("Found %d shards...", list_length(shardList))));

	foreach(cell, shardList)
	{
		ListCell *placementCell = NULL;
		int64 *shardId = NULL;

		shardId = (int64 *) lfirst(cell);
		PgsShard * shard = PgsLoadShard(*shardId);

		ereport(INFO, (errmsg("Shard #" INT64_FORMAT, shard->id)));
		ereport(INFO,
				(errmsg("\trelation:\t%s", get_rel_name(shard->relationId))));
		ereport(INFO, (errmsg("\tmin value:\t%d", shard->minValue)));
		ereport(INFO, (errmsg("\tmax value:\t%d", shard->maxValue)));

		placementList = PgsLoadPlacementList(*shardId);
		ereport(INFO, (errmsg("\t%d placements:", list_length(placementList))));

		foreach(placementCell, placementList)

		{
			PgsPlacement *placement = NULL;

			placement = (PgsPlacement *) lfirst(placementCell);

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
 * PgsLoadShardList returns a List of shard identifiers related to a given
 * distributed table. If no shards can be found for the specified relation,
 * an empty List is returned.
 */
List *
PgsLoadShardList(Oid relationId)
{
	const int scanKeyCount = 1;

	List *shardList = NIL;

	RangeVar *rv = NULL;
	Relation rel = NULL;
	HeapScanDesc scanDesc = NULL;
	ScanKeyData scanKey[scanKeyCount];
	HeapTuple tup = NULL;
	bool isNull = false;

	rv = makeRangeVar(METADATA_SCHEMA, SHARD_TABLE, -1);

	rel = relation_openrv(rv, AccessShareLock);

	ScanKeyInit(&scanKey[0], ATTR_NUM_SHARD_RELATION_ID,
				InvalidStrategy, F_OIDEQ, ObjectIdGetDatum(relationId));

	scanDesc = heap_beginscan(rel, SnapshotNow, scanKeyCount, scanKey);

	while (HeapTupleIsValid(tup = heap_getnext(scanDesc, ForwardScanDirection)))
	{
		TupleDesc tupleDesc = RelationGetDescr(rel);
		Datum shardIdDatum = heap_getattr(tup, ATTR_NUM_SHARD_ID, tupleDesc,
										  &isNull);

		int64 shardId = DatumGetInt64(shardIdDatum);
		int64 *shardIdPointer = (int64 *) palloc0(sizeof(int64));
		*shardIdPointer = shardId;

		shardList = lappend(shardList, shardIdPointer);
	}

	heap_endscan(scanDesc);
	relation_close(rel, AccessShareLock);

	return shardList;
}


/*
 * PgsLoadShard collects metadata for a specified shard in a PgsShard
 * and returns a pointer to that structure. If no shard can be found using the
 * provided identifier, an error is thrown.
 */
PgsShard *
PgsLoadShard(int64 shardId)
{
	const int scanKeyCount = 1;

	RangeVar *rv = NULL;
	Relation rel = NULL;
	HeapScanDesc scanDesc = NULL;
	ScanKeyData scanKey[scanKeyCount];
	HeapTuple tup = NULL;
	PgsShard *shard = NULL;

	rv = makeRangeVar(METADATA_SCHEMA, SHARD_TABLE, -1);

	rel = relation_openrv(rv, AccessShareLock);

	ScanKeyInit(&scanKey[0], ATTR_NUM_SHARD_ID,
				InvalidStrategy, F_INT8EQ, Int64GetDatum(shardId));

	scanDesc = heap_beginscan(rel, SnapshotNow, scanKeyCount, scanKey);

	if (HeapTupleIsValid(tup = heap_getnext(scanDesc, ForwardScanDirection)))
	{
		TupleDesc tupleDesc = RelationGetDescr(rel);
		shard = TupleToShard(tup, tupleDesc);
	}
	else
	{
		ereport(ERROR, (errmsg("could not find entry for shard " INT64_FORMAT,
						shardId)));
	}

	heap_endscan(scanDesc);
	relation_close(rel, AccessShareLock);

	return shard;
}


/*
 * PgsLoadPlacementList gathers placement metadata for every placement of a
 * given shard and returns a List of PgsPlacements containing that metadata.
 * If no placements exist for the specified shard, an empty list is returned.
 */
List *
PgsLoadPlacementList(int64 shardId)
{
	const int scanKeyCount = 1;

	List *placementList = NIL;

	RangeVar *rv = NULL;
	Relation rel = NULL;
	HeapScanDesc scanDesc = NULL;
	ScanKeyData scanKey[scanKeyCount];
	HeapTuple tup = NULL;

	rv = makeRangeVar(METADATA_SCHEMA, PLACEMENT_TABLE, -1);

	rel = relation_openrv(rv, AccessShareLock);

	ScanKeyInit(&scanKey[0], ATTR_NUM_PLACEMENT_SHARD_ID,
				InvalidStrategy, F_INT8EQ, Int64GetDatum(shardId));

	scanDesc = heap_beginscan(rel, SnapshotNow, scanKeyCount, scanKey);

	while (HeapTupleIsValid(tup = heap_getnext(scanDesc, ForwardScanDirection)))
	{
		TupleDesc tupleDesc = RelationGetDescr(rel);
		PgsPlacement *placement = TupleToPlacement(tup, tupleDesc);
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


/*
 * TupleToShard populates a PgsShard using values from a row of the shards
 * configuration table and returns a pointer to that struct. The input tuple
 * must not contain any NULLs.
 */
static PgsShard *
TupleToShard(HeapTuple tup, TupleDesc tupleDesc)
{
	PgsShard *shard = NULL;

	bool isNull = false;

	Datum idDatum = heap_getattr(tup, ATTR_NUM_SHARD_ID, tupleDesc, &isNull);
	Datum relationIdDatum = heap_getattr(tup, ATTR_NUM_SHARD_RELATION_ID,
										 tupleDesc, &isNull);
	Datum minValueDatum = heap_getattr(tup, ATTR_NUM_SHARD_MIN_VALUE, tupleDesc,
									   &isNull);
	Datum maxValueDatum = heap_getattr(tup, ATTR_NUM_SHARD_MAX_VALUE, tupleDesc,
									   &isNull);

	Assert(!HeapTupleHasNulls(tup));

	shard = palloc0(sizeof(PgsShard));
	shard->id = DatumGetInt64(idDatum);
	shard->relationId = DatumGetObjectId(relationIdDatum);
	shard->minValue = DatumGetInt32(minValueDatum);
	shard->maxValue = DatumGetInt32(maxValueDatum);

	return shard;
}


/*
 * TupleToPlacement populates a PgsPlacement using values from a row of the
 * placements configuration table and returns a pointer to that struct. The
 * input tuple must not contain any NULLs.
 */
static PgsPlacement *
TupleToPlacement(HeapTuple tup, TupleDesc tupleDesc)
{
	PgsPlacement *placement = NULL;
	bool isNull = false;

	Datum idDatum = heap_getattr(tup, ATTR_NUM_PLACEMENT_ID, tupleDesc,
								 &isNull);
	Datum shardIdDatum = heap_getattr(tup, ATTR_NUM_PLACEMENT_SHARD_ID,
									  tupleDesc, &isNull);
	Datum hostDatum = heap_getattr(tup, ATTR_NUM_PLACEMENT_HOST, tupleDesc,
								   &isNull);
	Datum portDatm = heap_getattr(tup, ATTR_NUM_PLACEMENT_PORT, tupleDesc,
								  &isNull);

	Assert(!HeapTupleHasNulls(tup));

	placement = palloc0(sizeof(PgsPlacement));
	placement->id = DatumGetInt64(idDatum);
	placement->shardId = DatumGetInt64(shardIdDatum);
	placement->host = TextDatumGetCString(hostDatum);
	placement->port = DatumGetInt32(portDatm);

	return placement;
}
