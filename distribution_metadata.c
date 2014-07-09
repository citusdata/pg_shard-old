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
static Var * ColumnNameToColumn(Oid relationId, char *columnName);
static void LoadShardRow(int64 shardId, Oid *relationId, char **minValue,
						 char **maxValue);
static ShardPlacement * TupleToShardPlacement(HeapTuple heapTuple,
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
	List *shardPlacementList = NIL;

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

		shardPlacementList = LoadShardPlacementList(*shardId);
		ereport(INFO, (errmsg("\t%d placements:",
							  list_length(shardPlacementList))));

		foreach(placementCell, shardPlacementList)

		{
			ShardPlacement *shardPlacement = NULL;

			shardPlacement = (ShardPlacement *) lfirst(placementCell);

			ereport(INFO, (errmsg("\t\tPlacement #" INT64_FORMAT,
								  shardPlacement->id)));
			ereport(INFO, (errmsg("\t\t\tshard:\t" INT64_FORMAT,
								  shardPlacement->shardId)));
			ereport(INFO, (errmsg("\t\t\tnode name:\t%s",
								  shardPlacement->nodeName)));
			ereport(INFO, (errmsg("\t\t\tnode port:\t%u",
								  shardPlacement->nodePort)));
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
		bool isNull = false;

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
	char *minValueString = NULL;
	char *maxValueString = NULL;

	/* first read the related row from the shard table */
	LoadShardRow(shardId, &relationId, &minValueString, &maxValueString);

	/* then find min/max values' actual types */
	partitionColumn = PartitionColumn(relationId);
	getTypeInputInfo(partitionColumn->vartype, &inputFunctionId, &typeIoParam);

	/* finally convert min/max values to their actual types */
	minValue = OidInputFunctionCall(inputFunctionId, minValueString,
									typeIoParam, partitionColumn->vartypmod);
	maxValue = OidInputFunctionCall(inputFunctionId, maxValueString,
									typeIoParam, partitionColumn->vartypmod);

	shard = (Shard *) palloc0(sizeof(Shard));
	shard->id = shardId;
	shard->relationId = relationId;
	shard->minValue = minValue;
	shard->maxValue = maxValue;
	shard->valueTypeId = INT4OID;	/* we only deal with hash ranges for now */

	return shard;
}


/*
 * LoadShardPlacementList gathers metadata for every placement of a given shard
 * and returns a List of ShardPlacements containing that metadata. If the
 * specified shard has not been placed, an error is thrown.
 */
List *
LoadShardPlacementList(int64 shardId)
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
		ShardPlacement *shardPlacement =
				TupleToShardPlacement(heapTuple, tupleDescriptor);
		placementList = lappend(placementList, shardPlacement);

		heapTuple = index_getnext(indexScanDesc, ForwardScanDirection);
	}

	index_endscan(indexScanDesc);
	index_close(indexRelation, AccessShareLock);
	relation_close(heapRelation, AccessShareLock);

	/* if no shard placements are found, error out */
	if (placementList == NIL)
	{
		ereport(ERROR, (errmsg("could not find any placements for shardId "
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

	RangeVar *heapRangeVar = NULL;
	Relation heapRelation = NULL;
	HeapScanDesc scanDesc = NULL;
	ScanKeyData scanKey[scanKeyCount];
	HeapTuple heapTuple = NULL;

	Var *partitionColumn = NULL;

	heapRangeVar = makeRangeVar(METADATA_SCHEMA, PARTITION_STRATEGY_TABLE_NAME, -1);

	heapRelation = relation_openrv(heapRangeVar, AccessShareLock);

	ScanKeyInit(&scanKey[0], ATTR_NUM_PARTITION_STRATEGY_RELATION_ID,
			InvalidStrategy, F_OIDEQ, ObjectIdGetDatum(distributedTableId));

	scanDesc = heap_beginscan(heapRelation, SnapshotNow, scanKeyCount, scanKey);

	// TODO: Do I need to check scan->xs_recheck and recheck scan key?
	heapTuple = heap_getnext(scanDesc, ForwardScanDirection);
	if (HeapTupleIsValid(heapTuple))
	{
		TupleDesc tupleDescriptor = RelationGetDescr(heapRelation);
		bool isNull = false;

		Datum keyDatum = heap_getattr(heapTuple,
									  ATTR_NUM_PARTITION_STRATEGY_KEY,
									  tupleDescriptor, &isNull);
		char *partitionColumnName = TextDatumGetCString(keyDatum);

		partitionColumn = ColumnNameToColumn(distributedTableId,
											 partitionColumnName);
	}
	else
	{
		ereport(ERROR, (errmsg("could not find partition for distributed"
							   "relation %u", distributedTableId)));
	}

	heap_endscan(scanDesc);
	relation_close(heapRelation, AccessShareLock);

	return partitionColumn;
}


/*
 * ColumnNameToColumn accepts a relation identifier and column name and returns
 * a Var that represents that column in that relation. If the column doesn't
 * exist or is a system column, an error is thrown.
 */
static Var *
ColumnNameToColumn(Oid relationId, char *columnName)
{
	Var *partitionColumn = NULL;

	Oid typid = InvalidOid;
	int32 vartypmod = -1;
	Oid collid = InvalidOid;

	/* dummy indexes needed by makeVar */
	const Index varno = 1;
	const Index varlevelsup = 0;

	AttrNumber attNum = get_attnum(relationId, columnName);

	if (attNum == InvalidAttrNumber)
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("partition column \"%s\" not found", columnName)));
	}
	else if (!AttrNumberIsForUserDefinedAttr(attNum))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
				 errmsg("specified partition column \"%s\" is a system column",
						 columnName)));
	}

	get_atttypetypmodcoll(relationId, attNum, &typid, &vartypmod, &collid);
	partitionColumn = makeVar(varno, attNum, typid, vartypmod, collid,
							  varlevelsup);

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
		bool isNull = false;

		Datum relationIdDatum = heap_getattr(heapTuple,
											 ATTR_NUM_SHARD_RELATION_ID,
											 tupleDescriptor, &isNull);
		Datum minValueDatum = heap_getattr(heapTuple, ATTR_NUM_SHARD_MIN_VALUE,
									 tupleDescriptor, &isNull);
		Datum maxValueDatum = heap_getattr(heapTuple, ATTR_NUM_SHARD_MAX_VALUE,
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
 * TupleToShardPlacement populates a ShardPlacement using values from a row of
 * the placements configuration table and returns a pointer to that struct. The
 * input tuple must not contain any NULLs.
 */
static ShardPlacement *
TupleToShardPlacement(HeapTuple heapTuple, TupleDesc tupleDescriptor)
{
	ShardPlacement *shardPlacement = NULL;
	bool isNull = false;

	Datum idDatum = heap_getattr(heapTuple, ATTR_NUM_PLACEMENT_ID,
								 tupleDescriptor, &isNull);
	Datum shardIdDatum = heap_getattr(heapTuple, ATTR_NUM_PLACEMENT_SHARD_ID,
									  tupleDescriptor, &isNull);
	Datum nodeNameDatum = heap_getattr(heapTuple, ATTR_NUM_PLACEMENT_NODE_NAME,
									   tupleDescriptor, &isNull);
	Datum nodePortDatum = heap_getattr(heapTuple, ATTR_NUM_PLACEMENT_NODE_PORT,
									   tupleDescriptor, &isNull);

	shardPlacement = palloc0(sizeof(ShardPlacement));
	shardPlacement->id = DatumGetInt64(idDatum);
	shardPlacement->shardId = DatumGetInt64(shardIdDatum);
	shardPlacement->nodeName = TextDatumGetCString(nodeNameDatum);
	shardPlacement->nodePort = DatumGetInt32(nodePortDatum);

	return shardPlacement;
}
