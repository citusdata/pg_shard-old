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
#include "commands/defrem.h"
#include "executor/executor.h"
#include "foreign/foreign.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "parser/parse_node.h"
#include "parser/parse_relation.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/palloc.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/tqual.h"

/* Returns a pointer to a newly palloc'd int64 with the value from src */
static int64 * AllocateInt64(int64 src);

/* Returns a shard populated with values from a given tuple */
static TopsieShard * TupleToShard(HeapTuple tup, TupleDesc tupleDesc);

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
	Oid relationId = PG_GETARG_OID(0);

	List *shardList = TopsieLoadShardList(relationId);
	List *placementList = NIL;

	ListCell   *cell = NULL;

	ereport(INFO, (errmsg("Found %d shards...", list_length(shardList))));

	foreach(cell, shardList)
	{
		ListCell *placementCell = NULL;
		int64 *shardId = NULL;

		shardId = (int64 *) lfirst(cell);
		TopsieShard * shard = TopsieLoadShard(*shardId);

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
 * Return a Var representing the column used to partition a given relation.
 */
Var *
TopsiePartitionColumn(Relation rel)
{
	Var *partitionColumn;

	/* Flags for addRangeTableEntryForRelation incovation. */
	const bool useInheritance = false;
	const bool inFromClause = true;

	/*
	 * Flags for addRTEtoQuery invocation. Only need to search column names, so
	 * don't bother adding relation name to parse state.
	 */
	const bool addToJoins = false;
	const bool addToNamespace = false;
	const bool addToVarNamespace = true;

	char *partitionColName = "";

	ListCell   *optionCell = NULL;
	ForeignTable *table = GetForeignTable(RelationGetRelid(rel));

	foreach(optionCell, table->options)
	{
		DefElem    *option = (DefElem *) lfirst(optionCell);

		if (strcmp(option->defname, "partition_column") == 0)
		{
			partitionColName = defGetString(option);
		}
	}

	// TODO: Error early if no partition column setting is found?

	ParseState *parseState = make_parsestate(NULL);

	RangeTblEntry *rte = addRangeTableEntryForRelation(parseState, rel, NULL,
			useInheritance, inFromClause);
	addRTEtoQuery(parseState, rte, addToJoins, addToNamespace, addToVarNamespace);

	partitionColumn = (Var *) scanRTEForColumn(parseState, rte,
			partitionColName, 0);

	free_parsestate(parseState);

	if (partitionColumn == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_COLUMN_NAME_NOT_FOUND),
				 errmsg("partition column \"%s\" not found", partitionColName)));
	}
	else if (!AttrNumberIsForUserDefinedAttr(partitionColumn->varattno))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_COLUMN_NAME),
				 errmsg("specified partition column \"%s\" is a system column",
						 partitionColName)));
	}

	return partitionColumn;
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
	TopsieShard *shard = NULL;

	rv = makeRangeVarFromNameList(
			stringToQualifiedNameList(METADATA_SCHEMA "." SHARDS_TABLE));

	rel = relation_openrv(rv, AccessShareLock);

	tupleDesc = RelationGetDescr(rel);

	ScanKeyInit(&scanKey[0], ANUM_SHARDS_ID,
			InvalidStrategy, F_INT8EQ, Int64GetDatum(shardId));

	scanDesc = heap_beginscan(rel, SnapshotNow, scanKeyCount, scanKey);

	if (HeapTupleIsValid(tup = heap_getnext(scanDesc, ForwardScanDirection)))
	{
		shard = TupleToShard(tup, tupleDesc);
	}
	else
	{
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

static TopsieShard *
TupleToShard(HeapTuple tup, TupleDesc tupleDesc)
{
	TopsieShard *shard = NULL;

	bool isNull = false;

	Datum idDatum = heap_getattr(tup, ANUM_SHARDS_ID, tupleDesc,
			&isNull);
	Datum relationIdDatum = heap_getattr(tup, ANUM_SHARDS_RELATION_ID, tupleDesc,
			&isNull);
	Datum minValueDatum = heap_getattr(tup, ANUM_SHARDS_MIN_VALUE, tupleDesc,
			&isNull);
	Datum maxValueDatum = heap_getattr(tup, ANUM_SHARDS_MAX_VALUE, tupleDesc,
			&isNull);

	Assert(!HeapTupleHasNulls(tup));

	shard = palloc0(sizeof(TopsieShard));
	shard->id = DatumGetInt64(idDatum);
	shard->relationId = DatumGetObjectId(relationIdDatum);
	shard->minValue = DatumGetInt32(minValueDatum);
	shard->maxValue = DatumGetInt32(maxValueDatum);

	return shard;
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
