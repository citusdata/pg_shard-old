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

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/htup.h"
#include "access/skey.h"
#include "executor/executor.h"
#include "nodes/makefuncs.h"
#include "nodes/params.h"
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
#include "utils/tqual.h"


/* local function forward declarations */
static Var * ColumnNameToVar(Relation relation, char *columnName);
static PgsShard * TupleToShard(HeapTuple tuple, TupleDesc tupleDescriptor);
static PgsPlacement * TupleToPlacement(HeapTuple tuple,
									   TupleDesc tupleDescriptor);


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

	Var *partitionColumn = PgsPartitionColumn(relationId);
	List *shardList = PgsLoadShardList(relationId);
	List *placementList = NIL;

	ListCell *cell = NULL;

	ereport(INFO, (errmsg("Table is partitioned using column #%d, "
						  "which is of type \"%s\"", partitionColumn->varattno,
						  format_type_be(partitionColumn->vartype))));

	ereport(INFO, (errmsg("Found %d shards...", list_length(shardList))));

	foreach(cell, shardList)
	{
		ListCell *placementCell = NULL;
		int64 *shardId = NULL;

		shardId = (int64 *) lfirst(cell);
		PgsShard * shard = PgsLoadShard(*shardId);

		ereport(INFO, (errmsg("Shard #" INT64_FORMAT, shard->id)));
		ereport(INFO, (errmsg("\trelation:\t%s",
							  get_rel_name(shard->relationId))));
		ereport(INFO, (errmsg("\tmin value:\t%d", shard->minValue)));
		ereport(INFO, (errmsg("\tmax value:\t%d", shard->maxValue)));

		placementList = PgsLoadPlacementList(*shardId);
		ereport(INFO, (errmsg("\t%d placements:",
							  list_length(placementList))));

		foreach(placementCell, placementList)

		{
			PgsPlacement *placement = NULL;

			placement = (PgsPlacement *) lfirst(placementCell);

			ereport(INFO, (errmsg("\t\tPlacement #" INT64_FORMAT,
								  placement->id)));
			ereport(INFO, (errmsg("\t\t\tshard:\t" INT64_FORMAT,
								  placement->shardId)));
			ereport(INFO, (errmsg("\t\t\tnode name:\t%s",
								  placement->nodeName)));
			ereport(INFO, (errmsg("\t\t\tnode port:\t%u",
								  placement->nodePort)));
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

	RangeVar *heapRangeVar = NULL, *indexRangeVar = NULL;
	Relation heapRelation = NULL, indexRelation = NULL;
	IndexScanDesc idxScanDesc = NULL;
	ScanKeyData scanKey[scanKeyCount];
	HeapTuple tuple = NULL;
	bool isNull = false;

	heapRangeVar = makeRangeVar(METADATA_SCHEMA, SHARD_TABLE_NAME, -1);
	indexRangeVar = makeRangeVar(METADATA_SCHEMA, SHARD_RELATION_IDX, -1);

	heapRelation = relation_openrv(heapRangeVar, AccessShareLock);
	indexRelation = relation_openrv(indexRangeVar, AccessShareLock);

	ScanKeyInit(&scanKey[0], 1, BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relationId));

	idxScanDesc = index_beginscan(heapRelation, indexRelation, SnapshotNow,
								  scanKeyCount, 0);
	index_rescan(idxScanDesc, scanKey, scanKeyCount, NULL, 0);

	// TODO: Do I need to check scan->xs_recheck and recheck scan key?
	tuple = index_getnext(idxScanDesc, ForwardScanDirection);
	while (HeapTupleIsValid(tuple))
	{
		TupleDesc tupleDescriptor = RelationGetDescr(heapRelation);
		Datum shardIdDatum = heap_getattr(tuple, ATTR_NUM_SHARD_ID,
										  tupleDescriptor, &isNull);

		int64 shardId = DatumGetInt64(shardIdDatum);
		int64 *shardIdPointer = (int64 *) palloc0(sizeof(int64));
		*shardIdPointer = shardId;

		shardList = lappend(shardList, shardIdPointer);

		tuple = index_getnext(idxScanDesc, ForwardScanDirection);
	}

	index_endscan(idxScanDesc);
	index_close(indexRelation, AccessShareLock);
	relation_close(heapRelation, AccessShareLock);

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

	RangeVar *heapRangeVar = NULL, *indexRangeVar = NULL;
	Relation heapRelation = NULL, indexRelation = NULL;
	IndexScanDesc idxScanDesc = NULL;
	ScanKeyData scanKey[scanKeyCount];
	HeapTuple tuple = NULL;

	PgsShard *shard = NULL;

	heapRangeVar = makeRangeVar(METADATA_SCHEMA, SHARD_TABLE_NAME, -1);
	indexRangeVar = makeRangeVar(METADATA_SCHEMA, SHARD_PKEY_IDX, -1);

	heapRelation = relation_openrv(heapRangeVar, AccessShareLock);
	indexRelation = relation_openrv(indexRangeVar, AccessShareLock);

	ScanKeyInit(&scanKey[0], 1, BTEqualStrategyNumber, F_INT8EQ,
				Int64GetDatum(shardId));

	idxScanDesc = index_beginscan(heapRelation, indexRelation, SnapshotNow,
								  scanKeyCount, 0);
	index_rescan(idxScanDesc, scanKey, scanKeyCount, NULL, 0);

	// TODO: Do I need to check scan->xs_recheck and recheck scan key?
	tuple = index_getnext(idxScanDesc, ForwardScanDirection);
	if (HeapTupleIsValid(tuple))
	{
		TupleDesc tupleDescriptor = RelationGetDescr(heapRelation);
		shard = TupleToShard(tuple, tupleDescriptor);
	}
	else
	{
		ereport(ERROR, (errmsg("could not find entry for shard " INT64_FORMAT,
						shardId)));
	}

	index_endscan(idxScanDesc);
	index_close(indexRelation, AccessShareLock);
	relation_close(heapRelation, AccessShareLock);

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

	RangeVar *heapRangeVar = NULL, *indexRangeVar = NULL;
	Relation heapRelation = NULL, indexRelation = NULL;
	IndexScanDesc idxScanDesc = NULL;
	ScanKeyData scanKey[scanKeyCount];
	HeapTuple tuple = NULL;

	heapRangeVar = makeRangeVar(METADATA_SCHEMA, PLACEMENT_TABLE_NAME, -1);
	indexRangeVar = makeRangeVar(METADATA_SCHEMA, PLACEMENT_SHARD_IDX, -1);

	heapRelation = relation_openrv(heapRangeVar, AccessShareLock);
	indexRelation = relation_openrv(indexRangeVar, AccessShareLock);

	ScanKeyInit(&scanKey[0], 1, BTEqualStrategyNumber, F_INT8EQ,
				Int64GetDatum(shardId));

	idxScanDesc = index_beginscan(heapRelation, indexRelation, SnapshotNow,
								  scanKeyCount, 0);
	index_rescan(idxScanDesc, scanKey, scanKeyCount, NULL, 0);

	// TODO: Do I need to check scan->xs_recheck and recheck scan key?
	tuple = index_getnext(idxScanDesc, ForwardScanDirection);
	while (HeapTupleIsValid(tuple))
	{
		TupleDesc tupleDescriptor = RelationGetDescr(heapRelation);
		PgsPlacement *placement = TupleToPlacement(tuple, tupleDescriptor);
		placementList = lappend(placementList, placement);

		tuple = index_getnext(idxScanDesc, ForwardScanDirection);
	}

	index_endscan(idxScanDesc);
	index_close(indexRelation, AccessShareLock);
	relation_close(heapRelation, AccessShareLock);

	/* if no shard placements are found, warn the user */
	if (placementList == NIL)
	{
		ereport(WARNING, (errmsg("could not find any placements for shardId "
								 INT64_FORMAT, shardId)));
	}

	return placementList;
}


/*
 * PgsPartitionColumn looks up the column used to partition a given distributed
 * table and returns a reference to a Var representing that column. If no entry
 * can be found using the provided identifer, an error is thrown.
 */
Var *
PgsPartitionColumn(Oid relationId)
{
	const int scanKeyCount = 1;

	RangeVar *heapRangeVar = NULL, *indexRangeVar = NULL;
	Relation heapRelation = NULL, indexRelation = NULL;
	IndexScanDesc idxScanDesc = NULL;
	ScanKeyData scanKey[scanKeyCount];
	HeapTuple tuple = NULL;

	Var *partitionColumn = NULL;

	heapRangeVar = makeRangeVar(METADATA_SCHEMA,
								PARTITION_STRATEGY_TABLE_NAME, -1);
	indexRangeVar = makeRangeVar(METADATA_SCHEMA,
								 PARTITION_STRATEGY_RELATION_IDX, -1);

	heapRelation = relation_openrv(heapRangeVar, AccessShareLock);
	indexRelation = relation_openrv(indexRangeVar, AccessShareLock);

	ScanKeyInit(&scanKey[0], 1, BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relationId));

	idxScanDesc = index_beginscan(heapRelation, indexRelation, SnapshotNow,
								  scanKeyCount, 0);
	index_rescan(idxScanDesc, scanKey, scanKeyCount, NULL, 0);

	// TODO: Do I need to check scan->xs_recheck and recheck scan key?
	tuple = index_getnext(idxScanDesc, ForwardScanDirection);
	if (HeapTupleIsValid(tuple))
	{
		bool isNull = false;
		TupleDesc tupleDescriptor = RelationGetDescr(heapRelation);

		Datum keyDatum = heap_getattr(tuple, ATTR_NUM_PARTITION_STRATEGY_KEY,
									  tupleDescriptor, &isNull);
		char *partitionColumnName = TextDatumGetCString(keyDatum);

		Relation relation = relation_open(relationId, AccessShareLock);
		partitionColumn = ColumnNameToVar(relation, partitionColumnName);
		relation_close(relation, AccessShareLock);
	}
	else
	{
		ereport(ERROR, (errmsg("could not find partition strategy for relation "
							   "%u", relationId)));
	}

	index_endscan(idxScanDesc);
	index_close(indexRelation, AccessShareLock);
	relation_close(heapRelation, AccessShareLock);

	return partitionColumn;
}



/*
 * TupleToShard populates a PgsShard using values from a row of the shards
 * configuration table and returns a pointer to that struct. The input tuple
 * must not contain any NULLs.
 */
static PgsShard *
TupleToShard(HeapTuple tuple, TupleDesc tupleDescriptor)
{
	PgsShard *shard = NULL;

	bool isNull = false;

	Datum idDatum = heap_getattr(tuple, ATTR_NUM_SHARD_ID, tupleDescriptor,
								 &isNull);
	Datum relationIdDatum = heap_getattr(tuple, ATTR_NUM_SHARD_RELATION_ID,
										 tupleDescriptor, &isNull);
	Datum minValueDatum = heap_getattr(tuple, ATTR_NUM_SHARD_MIN_VALUE,
									   tupleDescriptor, &isNull);
	Datum maxValueDatum = heap_getattr(tuple, ATTR_NUM_SHARD_MAX_VALUE,
									   tupleDescriptor, &isNull);

	Assert(!HeapTupleHasNulls(tuple));

	shard = palloc0(sizeof(PgsShard));
	shard->id = DatumGetInt64(idDatum);
	shard->relationId = DatumGetObjectId(relationIdDatum);
	shard->minValue = DatumGetInt32(minValueDatum);
	shard->maxValue = DatumGetInt32(maxValueDatum);

	return shard;
}


/*
 * ColumnNameToVar accepts a relation and column name and returns a Var that
 * represents that column in that relation. If the column doesn't exist or is
 * a system column, an error is thrown.
 */
static Var *
ColumnNameToVar(Relation relation, char *columnName)
{
	Var *partitionColumn = NULL;

	/* Flags for addRangeTableEntryForRelation invocation. */
	const bool useInheritance = false;
	const bool inFromClause = true;

	/*
	 * Flags for addRTEtoQuery invocation. Only need to search column names, so
	 * don't bother adding relation name to parse state.
	 */
	const bool addToJoins = false;
	const bool addToNamespace = false;
	const bool addToVarNamespace = true;

	ParseState *parseState = make_parsestate(NULL);

	RangeTblEntry *rte = addRangeTableEntryForRelation(parseState, relation,
													   NULL, useInheritance,
													   inFromClause);
	addRTEtoQuery(parseState, rte, addToJoins, addToNamespace,
				  addToVarNamespace);

	partitionColumn = (Var *) scanRTEForColumn(parseState, rte, columnName, 0);

	free_parsestate(parseState);

	if (partitionColumn == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("partition column \"%s\" not found", columnName)));
	}
	else if (!AttrNumberIsForUserDefinedAttr(partitionColumn->varattno))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
				 errmsg("specified partition column \"%s\" is a system column",
						 columnName)));
	}

	return partitionColumn;
}


/*
 * TupleToPlacement populates a PgsPlacement using values from a row of the
 * placements configuration table and returns a pointer to that struct. The
 * input tuple must not contain any NULLs.
 */
static PgsPlacement *
TupleToPlacement(HeapTuple tuple, TupleDesc tupleDescriptor)
{
	PgsPlacement *placement = NULL;
	bool isNull = false;

	Datum idDatum = heap_getattr(tuple, ATTR_NUM_PLACEMENT_ID, tupleDescriptor,
								 &isNull);
	Datum shardIdDatum = heap_getattr(tuple, ATTR_NUM_PLACEMENT_SHARD_ID,
									  tupleDescriptor, &isNull);
	Datum nodeNameDatum = heap_getattr(tuple, ATTR_NUM_PLACEMENT_NODE_NAME,
								   tupleDescriptor, &isNull);
	Datum nodePortDatum = heap_getattr(tuple, ATTR_NUM_PLACEMENT_NODE_PORT,
								  tupleDescriptor, &isNull);

	Assert(!HeapTupleHasNulls(tuple));

	placement = palloc0(sizeof(PgsPlacement));
	placement->id = DatumGetInt64(idDatum);
	placement->shardId = DatumGetInt64(shardIdDatum);
	placement->nodeName = TextDatumGetCString(nodeNameDatum);
	placement->nodePort = DatumGetInt32(nodePortDatum);

	return placement;
}
