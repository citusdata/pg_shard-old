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
#include "catalog/pg_type.h"
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
static void LoadShardRow(int64 shardId, Oid *relationId, char **minValue,
						 char **maxValue);
static Placement * TupleToPlacement(HeapTuple heapTuple,
									TupleDesc tupleDescriptor);


/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(TestDistributionMetadata);


/*
 * TestDistributionMetadata prints all current shard and placement configuration
 * at INFO level for testing purposes.
 */
Datum
TestDistributionMetadata(PG_FUNCTION_ARGS)
{
	Oid distributedTableId = PG_GETARG_OID(0);

	Var *partitionColumn = PartitionColumn(distributedTableId);
	List *shardList = LoadShardList(distributedTableId);
	List *placementList = NIL;

	ListCell *cell = NULL;

	FmgrInfo outputFunctionInfo = { };
	Oid outputFunctionId = InvalidOid;
	bool isVarlena = false;

	/* then find min/max values' actual types */
	getTypeOutputInfo(partitionColumn->vartype, &outputFunctionId, &isVarlena);
	fmgr_info(outputFunctionId, &outputFunctionInfo);

	ereport(INFO, (errmsg("Table is partitioned using column #%d, "
						  "which is of type \"%s\"", partitionColumn->varattno,
						  format_type_be(partitionColumn->vartype))));

	ereport(INFO, (errmsg("Found %d shards...", list_length(shardList))));

	foreach(cell, shardList)
	{
		ListCell *placementCell = NULL;
		int64 *shardId = NULL;

		shardId = (int64 *) lfirst(cell);
		Shard * shard = LoadShard(*shardId);

		char *minValueStr = OutputFunctionCall(&outputFunctionInfo, shard->minValue);
		char *maxValueStr = OutputFunctionCall(&outputFunctionInfo, shard->maxValue);

		ereport(INFO, (errmsg("Shard #" INT64_FORMAT, shard->id)));
		ereport(INFO, (errmsg("\trelation:\t%s",
							  get_rel_name(shard->relationId))));

		ereport(INFO, (errmsg("\tmin value:\t%s", minValueStr)));
		ereport(INFO, (errmsg("\tmax value:\t%s", maxValueStr)));

		placementList = LoadPlacementList(*shardId);
		ereport(INFO, (errmsg("\t%d placements:",
							  list_length(placementList))));

		foreach(placementCell, placementList)

		{
			Placement *placement = NULL;

			placement = (Placement *) lfirst(placementCell);

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
 * LoadShardList returns a List of shard identifiers related for a given
 * distributed table. If no shards can be found for the specified relation, an
 * empty List is returned.
 */
List *
LoadShardList(Oid distributedTableId)
{
	const int scanKeyCount = 1;

	List *shardList = NIL;

	RangeVar *heapRangeVar = NULL, *indexRangeVar = NULL;
	Relation heapRelation = NULL, indexRelation = NULL;
	IndexScanDesc indexScanDesc = NULL;
	ScanKeyData scanKey[scanKeyCount];
	HeapTuple heapTuple = NULL;
	bool isNull = false;

	heapRangeVar = makeRangeVar(METADATA_SCHEMA, SHARD_TABLE_NAME, -1);
	indexRangeVar = makeRangeVar(METADATA_SCHEMA, SHARD_RELATION_IDX, -1);

	heapRelation = relation_openrv(heapRangeVar, AccessShareLock);
	indexRelation = relation_openrv(indexRangeVar, AccessShareLock);

	ScanKeyInit(&scanKey[0], 1, BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(distributedTableId));

	indexScanDesc = index_beginscan(heapRelation, indexRelation, SnapshotNow,
								  scanKeyCount, 0);
	index_rescan(indexScanDesc, scanKey, scanKeyCount, NULL, 0);

	// TODO: Do I need to check scan->xs_recheck and recheck scan key?
	heapTuple = index_getnext(indexScanDesc, ForwardScanDirection);
	while (HeapTupleIsValid(heapTuple))
	{
		TupleDesc tupleDescriptor = RelationGetDescr(heapRelation);
		Datum shardIdDatum = heap_getattr(heapTuple, ATTR_NUM_SHARD_ID,
										  tupleDescriptor, &isNull);

		int64 shardId = DatumGetInt64(shardIdDatum);
		int64 *shardIdPointer = (int64 *) palloc0(sizeof(int64));
		*shardIdPointer = shardId;

		shardList = lappend(shardList, shardIdPointer);

		heapTuple = index_getnext(indexScanDesc, ForwardScanDirection);
	}

	index_endscan(indexScanDesc);
	index_close(indexRelation, AccessShareLock);
	relation_close(heapRelation, AccessShareLock);

	return shardList;
}


/*
 * LoadShard collects metadata for a specified shard in a Shard and returns a
 * pointer to that structure. If no shard can be found using the provided
 * identifier, an error is thrown.
 */
Shard *
LoadShard(int64 shardId)
{
	Shard *shard = NULL;
	Datum minValue = 0;
	Datum maxValue = 0;
	Var *partitionColumn = NULL;
	Oid inputFunctionId = InvalidOid;
	Oid typeIoParam = InvalidOid;
	Oid relationId = InvalidOid;
	FmgrInfo inputFunctionInfo = { };
	char *minValueString = NULL;
	char *maxValueString = NULL;

	/* first read the related row from the shard table */
	LoadShardRow(shardId, &relationId, &minValueString, &maxValueString);

	/* then find min/max values' actual types */
	partitionColumn = PartitionColumn(relationId);
	getTypeInputInfo(partitionColumn->vartype, &inputFunctionId, &typeIoParam);
	fmgr_info(inputFunctionId, &inputFunctionInfo);

	/* finally convert min/max values to their actual types */
	minValue = InputFunctionCall(&inputFunctionInfo, minValueString,
								 typeIoParam, partitionColumn->vartypmod);
	maxValue = InputFunctionCall(&inputFunctionInfo, maxValueString,
								 typeIoParam, partitionColumn->vartypmod);

	shard = (Shard *) palloc0(sizeof(Shard));
	shard->id = shardId;
	shard->relationId = relationId;
	shard->minValue = minValue;
	shard->maxValue = maxValue;
	shard->valueTypeId = INT4OID;	// we only deal with hash ranges for now

	return shard;
}


/*
 * LoadPlacementList gathers placement metadata for every placement of a given
 * shard and returns a List of Placements containing that metadata. If no
 * placements exist for the specified shard, an empty list is returned.
 */
List *
LoadPlacementList(int64 shardId)
{
	const int scanKeyCount = 1;

	List *placementList = NIL;

	RangeVar *heapRangeVar = NULL, *indexRangeVar = NULL;
	Relation heapRelation = NULL, indexRelation = NULL;
	IndexScanDesc indexScanDesc = NULL;
	ScanKeyData scanKey[scanKeyCount];
	HeapTuple heapTuple = NULL;

	heapRangeVar = makeRangeVar(METADATA_SCHEMA, PLACEMENT_TABLE_NAME, -1);
	indexRangeVar = makeRangeVar(METADATA_SCHEMA, PLACEMENT_SHARD_IDX, -1);

	heapRelation = relation_openrv(heapRangeVar, AccessShareLock);
	indexRelation = relation_openrv(indexRangeVar, AccessShareLock);

	ScanKeyInit(&scanKey[0], 1, BTEqualStrategyNumber, F_INT8EQ,
				Int64GetDatum(shardId));

	indexScanDesc = index_beginscan(heapRelation, indexRelation, SnapshotNow,
								  scanKeyCount, 0);
	index_rescan(indexScanDesc, scanKey, scanKeyCount, NULL, 0);

	// TODO: Do I need to check scan->xs_recheck and recheck scan key?
	heapTuple = index_getnext(indexScanDesc, ForwardScanDirection);
	while (HeapTupleIsValid(heapTuple))
	{
		TupleDesc tupleDescriptor = RelationGetDescr(heapRelation);
		Placement *placement = TupleToPlacement(heapTuple, tupleDescriptor);
		placementList = lappend(placementList, placement);

		heapTuple = index_getnext(indexScanDesc, ForwardScanDirection);
	}

	index_endscan(indexScanDesc);
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
 * PartitionColumn looks up the column used to partition a given distributed
 * table and returns a reference to a Var representing that column. If no entry
 * can be found using the provided identifer, an error is thrown.
 */
Var *
PartitionColumn(Oid distributedTableId)
{
	const int scanKeyCount = 1;

	RangeVar *heapRangeVar = NULL, *indexRangeVar = NULL;
	Relation heapRelation = NULL, indexRelation = NULL;
	IndexScanDesc indexScanDesc = NULL;
	ScanKeyData scanKey[scanKeyCount];
	HeapTuple heapTuple = NULL;

	Var *partitionColumn = NULL;

	heapRangeVar = makeRangeVar(METADATA_SCHEMA,
								PARTITION_STRATEGY_TABLE_NAME, -1);
	indexRangeVar = makeRangeVar(METADATA_SCHEMA,
								 PARTITION_STRATEGY_RELATION_IDX, -1);

	heapRelation = relation_openrv(heapRangeVar, AccessShareLock);
	indexRelation = relation_openrv(indexRangeVar, AccessShareLock);

	ScanKeyInit(&scanKey[0], 1, BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(distributedTableId));

	indexScanDesc = index_beginscan(heapRelation, indexRelation, SnapshotNow,
								  scanKeyCount, 0);
	index_rescan(indexScanDesc, scanKey, scanKeyCount, NULL, 0);

	// TODO: Do I need to check scan->xs_recheck and recheck scan key?
	heapTuple = index_getnext(indexScanDesc, ForwardScanDirection);
	if (HeapTupleIsValid(heapTuple))
	{
		bool isNull = false;
		TupleDesc tupleDescriptor = RelationGetDescr(heapRelation);

		Datum keyDatum = heap_getattr(heapTuple,
									  ATTR_NUM_PARTITION_STRATEGY_KEY,
									  tupleDescriptor, &isNull);
		char *partitionColumnName = TextDatumGetCString(keyDatum);

		Relation relation = relation_open(distributedTableId,
										  AccessShareLock);
		partitionColumn = ColumnNameToVar(relation, partitionColumnName);
		relation_close(relation, AccessShareLock);
	}
	else
	{
		ereport(ERROR, (errmsg("could not find partition for distributed"
							   "relation %u", distributedTableId)));
	}

	index_endscan(indexScanDesc);
	index_close(indexRelation, AccessShareLock);
	relation_close(heapRelation, AccessShareLock);

	return partitionColumn;
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
 * LoadShardRow finds the row for the specified shard identifier in the shard
 * table and copies values from that row into the provided output parameters.
 */
static void
LoadShardRow(int64 shardId, Oid *relationId, char **minValue, char **maxValue)
{
	const int scanKeyCount = 1;

	RangeVar *heapRangeVar = NULL, *indexRangeVar = NULL;
	Relation heapRelation = NULL, indexRelation = NULL;
	IndexScanDesc indexScanDesc = NULL;
	ScanKeyData scanKey[scanKeyCount];
	HeapTuple heapTuple = NULL;

	Datum relationIdDatum = 0;
	Datum minValueDatum = 0;
	Datum maxValueDatum = 0;
	bool isNull = false;

	heapRangeVar = makeRangeVar(METADATA_SCHEMA, SHARD_TABLE_NAME, -1);
	indexRangeVar = makeRangeVar(METADATA_SCHEMA, SHARD_PKEY_IDX, -1);

	heapRelation = relation_openrv(heapRangeVar, AccessShareLock);
	indexRelation = relation_openrv(indexRangeVar, AccessShareLock);

	ScanKeyInit(&scanKey[0], 1, BTEqualStrategyNumber, F_INT8EQ,
				Int64GetDatum(shardId));

	indexScanDesc = index_beginscan(heapRelation, indexRelation, SnapshotNow,
								  scanKeyCount, 0);
	index_rescan(indexScanDesc, scanKey, scanKeyCount, NULL, 0);

	// TODO: Do I need to check scan->xs_recheck and recheck scan key?
	heapTuple = index_getnext(indexScanDesc, ForwardScanDirection);
	if (HeapTupleIsValid(heapTuple))
	{
		TupleDesc tupleDescriptor = RelationGetDescr(heapRelation);

		relationIdDatum = heap_getattr(heapTuple, ATTR_NUM_SHARD_RELATION_ID,
									   tupleDescriptor, &isNull);
		minValueDatum = heap_getattr(heapTuple, ATTR_NUM_SHARD_MIN_VALUE,
									 tupleDescriptor, &isNull);
		maxValueDatum = heap_getattr(heapTuple, ATTR_NUM_SHARD_MAX_VALUE,
									 tupleDescriptor, &isNull);

		/* convert and deep copy row's values */
		(*relationId) = DatumGetObjectId(relationIdDatum);
		(*minValue) = TextDatumGetCString(minValueDatum);
		(*maxValue) = TextDatumGetCString(maxValueDatum);

	}
	else
	{
		ereport(ERROR, (errmsg("could not find entry for shard " INT64_FORMAT,
						shardId)));
	}

	index_endscan(indexScanDesc);
	index_close(indexRelation, AccessShareLock);
	relation_close(heapRelation, AccessShareLock);

	return;
}


/*
 * TupleToPlacement populates a Placement using values from a row of the
 * placements configuration table and returns a pointer to that struct. The
 * input tuple must not contain any NULLs.
 */
static Placement *
TupleToPlacement(HeapTuple heapTuple, TupleDesc tupleDescriptor)
{
	Placement *placement = NULL;
	bool isNull = false;

	Datum idDatum = heap_getattr(heapTuple, ATTR_NUM_PLACEMENT_ID,
								 tupleDescriptor, &isNull);
	Datum shardIdDatum = heap_getattr(heapTuple, ATTR_NUM_PLACEMENT_SHARD_ID,
									  tupleDescriptor, &isNull);
	Datum nodeNameDatum = heap_getattr(heapTuple, ATTR_NUM_PLACEMENT_NODE_NAME,
									   tupleDescriptor, &isNull);
	Datum nodePortDatum = heap_getattr(heapTuple, ATTR_NUM_PLACEMENT_NODE_PORT,
									   tupleDescriptor, &isNull);

	Assert(!HeapTupleHasNulls(heapTuple));

	placement = palloc0(sizeof(Placement));
	placement->id = DatumGetInt64(idDatum);
	placement->shardId = DatumGetInt64(shardIdDatum);
	placement->nodeName = TextDatumGetCString(nodeNameDatum);
	placement->nodePort = DatumGetInt32(nodePortDatum);

	return placement;
}
