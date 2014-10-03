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
#include "pg_config.h"

#include "distribution_metadata.h"

#include <stddef.h>
#include <string.h>

#include "access/attnum.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/htup.h"
#include "access/sdir.h"
#include "access/skey.h"
#include "access/tupdesc.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "commands/sequence.h"
#include "nodes/makefuncs.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "storage/lock.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/palloc.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/tqual.h"


/* local function forward declarations */
static Var * ColumnNameToColumn(Oid relationId, char *columnName);
static void LoadShardIntervalRow(int64 shardId, Oid *relationId,
								 char **minValue, char **maxValue);
static ShardPlacement * TupleToShardPlacement(HeapTuple heapTuple,
											  TupleDesc tupleDescriptor);


/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(TestDistributionMetadata);


/*
 * TestDistributionMetadata prints all current shard and shard placement
 * configuration at INFO level for testing purposes.
 */
Datum
TestDistributionMetadata(PG_FUNCTION_ARGS)
{
	Oid distributedTableId = PG_GETARG_OID(0);

	Var *partitionColumn = PartitionColumn(distributedTableId);
	List *shardList = LoadShardList(distributedTableId);
	List *shardPlacementList = NIL;

	ListCell *cell = NULL;

	FmgrInfo outputFunctionInfo;
	Oid outputFunctionId = InvalidOid;
	bool isVarlena = false;

	memset(&outputFunctionInfo, 0, sizeof(outputFunctionInfo));

	/* then find min/max values' actual types */
	getTypeOutputInfo(partitionColumn->vartype, &outputFunctionId, &isVarlena);
	fmgr_info(outputFunctionId, &outputFunctionInfo);

	ereport(INFO, (errmsg("Table partition using column #%d with type \"%s\"",
						  partitionColumn->varattno,
						  format_type_be(partitionColumn->vartype))));

	ereport(INFO, (errmsg("Found %d shards...", list_length(shardList))));

	foreach(cell, shardList)
	{
		ListCell *shardPlacementCell = NULL;
		int64 *shardId = (int64 *) lfirst(cell);

		ShardInterval *shardInterval = LoadShardInterval(*shardId);

		char *minValueStr = OutputFunctionCall(&outputFunctionInfo,
											   shardInterval->minValue);
		char *maxValueStr = OutputFunctionCall(&outputFunctionInfo,
											   shardInterval->maxValue);

		ereport(INFO, (errmsg("Shard Interval #" INT64_FORMAT, shardInterval->id)));
		ereport(INFO, (errmsg("\trelation:\t%s",
							  get_rel_name(shardInterval->relationId))));

		ereport(INFO, (errmsg("\tmin value:\t%s", minValueStr)));
		ereport(INFO, (errmsg("\tmax value:\t%s", maxValueStr)));

		shardPlacementList = LoadShardPlacementList(*shardId);
		ereport(INFO, (errmsg("\t%d shard placements:",
							  list_length(shardPlacementList))));

		foreach(shardPlacementCell, shardPlacementList)

		{
			ShardPlacement *shardPlacement = NULL;

			shardPlacement = (ShardPlacement *) lfirst(shardPlacementCell);

			ereport(INFO, (errmsg("\t\tShardPlacement #" INT64_FORMAT,
								  shardPlacement->id)));
			ereport(INFO, (errmsg("\t\t\tshard:\t" INT64_FORMAT,
								  shardPlacement->shardId)));
			ereport(INFO, (errmsg("\t\t\tnode name:\t%s", shardPlacement->nodeName)));
			ereport(INFO, (errmsg("\t\t\tnode port:\t%u", shardPlacement->nodePort)));
		}
	}

	PG_RETURN_VOID();
}


/*
 * LoadShardList returns a list of shard identifiers related for a given
 * distributed table. The function returns an empty list if no shards can be
 * found for the given relation.
 */
List *
LoadShardList(Oid distributedTableId)
{
	List *shardList = NIL;
	RangeVar *heapRangeVar = NULL;
	RangeVar *indexRangeVar = NULL;
	Relation heapRelation = NULL;
	Relation indexRelation = NULL;
	IndexScanDesc indexScanDesc = NULL;
	const int scanKeyCount = 1;
	ScanKeyData scanKey[scanKeyCount];
	HeapTuple heapTuple = NULL;

	heapRangeVar = makeRangeVar(METADATA_SCHEMA_NAME, SHARD_TABLE_NAME, -1);
	indexRangeVar = makeRangeVar(METADATA_SCHEMA_NAME, SHARD_RELATION_INDEX_NAME, -1);

	heapRelation = relation_openrv(heapRangeVar, AccessShareLock);
	indexRelation = relation_openrv(indexRangeVar, AccessShareLock);

	ScanKeyInit(&scanKey[0], 1, BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(distributedTableId));

	indexScanDesc = index_beginscan(heapRelation, indexRelation, SnapshotNow,
									scanKeyCount, 0);
	index_rescan(indexScanDesc, scanKey, scanKeyCount, NULL, 0);

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
 * LoadShardInterval collects metadata for a specified shard in a ShardInterval
 * and returns a pointer to that structure. The function throws an error if no
 * shard can be found using the provided identifier.
 */
ShardInterval *
LoadShardInterval(int64 shardId)
{
	ShardInterval *shardInterval = NULL;
	Datum minValue = 0;
	Datum maxValue = 0;
	Var *partitionColumn = NULL;
	Oid inputFunctionId = InvalidOid;
	Oid typeIoParam = InvalidOid;
	Oid relationId = InvalidOid;
	char *minValueString = NULL;
	char *maxValueString = NULL;

	/* first read the related row from the shard table */
	LoadShardIntervalRow(shardId, &relationId, &minValueString, &maxValueString);

	/* then find min/max values' actual types */
	partitionColumn = PartitionColumn(relationId);
	getTypeInputInfo(partitionColumn->vartype, &inputFunctionId, &typeIoParam);

	/* finally convert min/max values to their actual types */
	minValue = OidInputFunctionCall(inputFunctionId, minValueString,
									typeIoParam, partitionColumn->vartypmod);
	maxValue = OidInputFunctionCall(inputFunctionId, maxValueString,
									typeIoParam, partitionColumn->vartypmod);

	shardInterval = (ShardInterval *) palloc0(sizeof(ShardInterval));
	shardInterval->id = shardId;
	shardInterval->relationId = relationId;
	shardInterval->minValue = minValue;
	shardInterval->maxValue = maxValue;
	shardInterval->valueTypeId = INT4OID;	/* hardcoded for hash ranges */

	return shardInterval;
}


/*
 * LoadFinalizedShardPlacementList returns all placements for a given shard that
 * are in the finalized state. Like LoadShardPlacementList, this function throws
 * an error if the specified shard has not been placed.
 */
List *
LoadFinalizedShardPlacementList(uint64 shardId)
{
	List *finalizedPlacementList = NIL;
	List *shardPlacementList = LoadShardPlacementList(shardId);

	ListCell *shardPlacementCell = NULL;
	foreach(shardPlacementCell, shardPlacementList)
	{
		ShardPlacement *shardPlacement = (ShardPlacement *) lfirst(shardPlacementCell);
		if (shardPlacement->shardState == STATE_FINALIZED)
		{
			finalizedPlacementList = lappend(finalizedPlacementList, shardPlacement);
		}
	}

	return finalizedPlacementList;
}


/*
 * LoadShardPlacementList gathers metadata for every placement of a given shard
 * and returns a list of ShardPlacements containing that metadata. The function
 * throws an error if the specified shard has not been placed.
 */
List *
LoadShardPlacementList(int64 shardId)
{
	List *shardPlacementList = NIL;
	RangeVar *heapRangeVar = NULL;
	RangeVar *indexRangeVar = NULL;
	Relation heapRelation = NULL;
	Relation indexRelation = NULL;
	IndexScanDesc indexScanDesc = NULL;
	const int scanKeyCount = 1;
	ScanKeyData scanKey[scanKeyCount];
	HeapTuple heapTuple = NULL;

	heapRangeVar = makeRangeVar(METADATA_SCHEMA_NAME, SHARD_PLACEMENT_TABLE_NAME, -1);
	indexRangeVar = makeRangeVar(METADATA_SCHEMA_NAME,
								 SHARD_PLACEMENT_SHARD_INDEX_NAME, -1);

	heapRelation = relation_openrv(heapRangeVar, AccessShareLock);
	indexRelation = relation_openrv(indexRangeVar, AccessShareLock);

	ScanKeyInit(&scanKey[0], 1, BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(shardId));

	indexScanDesc = index_beginscan(heapRelation, indexRelation, SnapshotNow,
									scanKeyCount, 0);
	index_rescan(indexScanDesc, scanKey, scanKeyCount, NULL, 0);

	heapTuple = index_getnext(indexScanDesc, ForwardScanDirection);
	while (HeapTupleIsValid(heapTuple))
	{
		TupleDesc tupleDescriptor = RelationGetDescr(heapRelation);
		ShardPlacement *shardPlacement = TupleToShardPlacement(heapTuple,
															   tupleDescriptor);
		shardPlacementList = lappend(shardPlacementList, shardPlacement);

		heapTuple = index_getnext(indexScanDesc, ForwardScanDirection);
	}

	index_endscan(indexScanDesc);
	index_close(indexRelation, AccessShareLock);
	relation_close(heapRelation, AccessShareLock);

	/* if no shard placements are found, error out */
	if (shardPlacementList == NIL)
	{
		ereport(ERROR, (errmsg("could not find any placements for shardId "
							   INT64_FORMAT, shardId)));
	}

	return shardPlacementList;
}


/*
 * PartitionColumn looks up the column used to partition a given distributed
 * table and returns a reference to a Var representing that column. If no entry
 * can be found using the provided identifer, this function throws an error.
 */
Var *
PartitionColumn(Oid distributedTableId)
{
	Var *partitionColumn = NULL;
	RangeVar *heapRangeVar = NULL;
	Relation heapRelation = NULL;
	HeapScanDesc scanDesc = NULL;
	const int scanKeyCount = 1;
	ScanKeyData scanKey[scanKeyCount];
	HeapTuple heapTuple = NULL;

	heapRangeVar = makeRangeVar(METADATA_SCHEMA_NAME, PARTITION_TABLE_NAME, -1);
	heapRelation = relation_openrv(heapRangeVar, AccessShareLock);

	ScanKeyInit(&scanKey[0], ATTR_NUM_PARTITION_RELATION_ID, InvalidStrategy,
				F_OIDEQ, ObjectIdGetDatum(distributedTableId));

	scanDesc = heap_beginscan(heapRelation, SnapshotNow, scanKeyCount, scanKey);

	heapTuple = heap_getnext(scanDesc, ForwardScanDirection);
	if (HeapTupleIsValid(heapTuple))
	{
		TupleDesc tupleDescriptor = RelationGetDescr(heapRelation);
		bool isNull = false;

		Datum keyDatum = heap_getattr(heapTuple, ATTR_NUM_PARTITION_KEY,
									  tupleDescriptor, &isNull);
		char *partitionColumnName = TextDatumGetCString(keyDatum);

		partitionColumn = ColumnNameToColumn(distributedTableId, partitionColumnName);
	}
	else
	{
		ereport(ERROR, (errmsg("could not find partition for distributed "
							   "relation %u", distributedTableId)));
	}

	heap_endscan(scanDesc);
	relation_close(heapRelation, AccessShareLock);

	return partitionColumn;
}


/*
 * PartitionType looks up the type used to partition a given distributed
 * table and returns a char representing this type. If no entry can be found
 * using the provided identifer, this function throws an error.
 */
char
PartitionType(Oid distributedTableId)
{
	char partitionType = 0;
	RangeVar *heapRangeVar = NULL;
	Relation heapRelation = NULL;
	HeapScanDesc scanDesc = NULL;
	const int scanKeyCount = 1;
	ScanKeyData scanKey[scanKeyCount];
	HeapTuple heapTuple = NULL;

	heapRangeVar = makeRangeVar(METADATA_SCHEMA_NAME, PARTITION_TABLE_NAME, -1);
	heapRelation = relation_openrv(heapRangeVar, AccessShareLock);

	ScanKeyInit(&scanKey[0], ATTR_NUM_PARTITION_RELATION_ID, InvalidStrategy,
				F_OIDEQ, ObjectIdGetDatum(distributedTableId));

	scanDesc = heap_beginscan(heapRelation, SnapshotNow, scanKeyCount, scanKey);

	heapTuple = heap_getnext(scanDesc, ForwardScanDirection);
	if (HeapTupleIsValid(heapTuple))
	{
		TupleDesc tupleDescriptor = RelationGetDescr(heapRelation);
		bool isNull = false;

		Datum partitionTypeDatum = heap_getattr(heapTuple, ATTR_NUM_PARTITION_TYPE,
												tupleDescriptor, &isNull);
		partitionType = DatumGetChar(partitionTypeDatum);
	}
	else
	{
		ereport(ERROR, (errmsg("could not find partition for distributed "
							   "relation %u", distributedTableId)));
	}

	heap_endscan(scanDesc);
	relation_close(heapRelation, AccessShareLock);

	return partitionType;
}


/*
 * IsDistributedTable simply returns whether the specified table is distributed.
 */
bool
IsDistributedTable(Oid tableId)
{
	bool isDistributedTable = false;
	RangeVar *heapRangeVar = NULL;
	Relation heapRelation = NULL;
	HeapScanDesc scanDesc = NULL;
	const int scanKeyCount = 1;
	ScanKeyData scanKey[scanKeyCount];
	HeapTuple heapTuple = NULL;

	heapRangeVar = makeRangeVar(METADATA_SCHEMA_NAME, PARTITION_TABLE_NAME, -1);
	heapRelation = relation_openrv(heapRangeVar, AccessShareLock);

	ScanKeyInit(&scanKey[0], ATTR_NUM_PARTITION_RELATION_ID, InvalidStrategy,
				F_OIDEQ, ObjectIdGetDatum(tableId));

	scanDesc = heap_beginscan(heapRelation, SnapshotNow, scanKeyCount, scanKey);

	heapTuple = heap_getnext(scanDesc, ForwardScanDirection);

	isDistributedTable = HeapTupleIsValid(heapTuple);

	heap_endscan(scanDesc);
	relation_close(heapRelation, AccessShareLock);

	return isDistributedTable;
}


/*
 * ColumnNameToColumn accepts a relation identifier and column name and returns
 * a Var that represents that column in that relation. This function throws an
 * error if the column doesn't exist or is a system column.
 */
static Var *
ColumnNameToColumn(Oid relationId, char *columnName)
{
	Var *partitionColumn = NULL;
	Oid columnTypeOid = InvalidOid;
	int32 columnTypeMod = -1;
	Oid columnCollationOid = InvalidOid;

	/* dummy indexes needed by makeVar */
	const Index tableId = 1;
	const Index columnLevelsUp = 0;

	AttrNumber columnId = get_attnum(relationId, columnName);
	if (columnId == InvalidAttrNumber)
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
						errmsg("partition column \"%s\" not found", columnName)));
	}
	else if (!AttrNumberIsForUserDefinedAttr(columnId))
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
						errmsg("specified partition column \"%s\" is a system "
							   "column", columnName)));
	}

	get_atttypetypmodcoll(relationId, columnId, &columnTypeOid, &columnTypeMod,
						  &columnCollationOid);
	partitionColumn = makeVar(tableId, columnId, columnTypeOid, columnTypeMod,
							  columnCollationOid, columnLevelsUp);

	return partitionColumn;
}


/*
 * LoadShardIntervalRow finds the row for the specified shard identifier in the
 * shard table and copies values from that row into the provided output params.
 */
static void
LoadShardIntervalRow(int64 shardId, Oid *relationId, char **minValue,
					 char **maxValue)
{
	RangeVar *heapRangeVar = NULL;
	RangeVar *indexRangeVar = NULL;
	Relation heapRelation = NULL;
	Relation indexRelation = NULL;
	IndexScanDesc indexScanDesc = NULL;
	const int scanKeyCount = 1;
	ScanKeyData scanKey[scanKeyCount];
	HeapTuple heapTuple = NULL;

	heapRangeVar = makeRangeVar(METADATA_SCHEMA_NAME, SHARD_TABLE_NAME, -1);
	indexRangeVar = makeRangeVar(METADATA_SCHEMA_NAME, SHARD_PKEY_INDEX_NAME, -1);

	heapRelation = relation_openrv(heapRangeVar, AccessShareLock);
	indexRelation = relation_openrv(indexRangeVar, AccessShareLock);

	ScanKeyInit(&scanKey[0], 1, BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(shardId));

	indexScanDesc = index_beginscan(heapRelation, indexRelation, SnapshotNow,
									scanKeyCount, 0);
	index_rescan(indexScanDesc, scanKey, scanKeyCount, NULL, 0);

	heapTuple = index_getnext(indexScanDesc, ForwardScanDirection);
	if (HeapTupleIsValid(heapTuple))
	{
		TupleDesc tupleDescriptor = RelationGetDescr(heapRelation);
		bool isNull = false;

		Datum relationIdDatum = heap_getattr(heapTuple, ATTR_NUM_SHARD_RELATION_ID,
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

	Datum idDatum = heap_getattr(heapTuple, ATTR_NUM_SHARD_PLACEMENT_ID,
								 tupleDescriptor, &isNull);
	Datum shardIdDatum = heap_getattr(heapTuple, ATTR_NUM_SHARD_PLACEMENT_SHARD_ID,
									  tupleDescriptor, &isNull);
	Datum shardStateDatum = heap_getattr(heapTuple, ATTR_NUM_SHARD_PLACEMENT_SHARD_STATE,
	                                     tupleDescriptor, &isNull);
	Datum nodeNameDatum = heap_getattr(heapTuple, ATTR_NUM_SHARD_PLACEMENT_NODE_NAME,
									   tupleDescriptor, &isNull);
	Datum nodePortDatum = heap_getattr(heapTuple, ATTR_NUM_SHARD_PLACEMENT_NODE_PORT,
									   tupleDescriptor, &isNull);

	shardPlacement = palloc0(sizeof(ShardPlacement));
	shardPlacement->id = DatumGetInt64(idDatum);
	shardPlacement->shardId = DatumGetInt64(shardIdDatum);
	shardPlacement->shardState = DatumGetInt32(shardStateDatum);
	shardPlacement->nodeName = TextDatumGetCString(nodeNameDatum);
	shardPlacement->nodePort = DatumGetInt32(nodePortDatum);

	return shardPlacement;
}


/*
 * InsertPartitionRow opens the partition metadata table and inserts a new row
 * with the given values.
 */
void
InsertPartitionRow(Oid distributedTableId, char partitionType, text *partitionKeyText)
{
	Relation partitionRelation = NULL;
	RangeVar *partitionRangeVar = NULL;
	TupleDesc tupleDescriptor = NULL;
	HeapTuple heapTuple = NULL;
	Datum values[PARTITION_TABLE_ATTRIBUTE_COUNT];
	bool isNulls[PARTITION_TABLE_ATTRIBUTE_COUNT];

	/* form new partition tuple */
	memset(values, 0, sizeof(values));
	memset(isNulls, false, sizeof(isNulls));

	values[ATTR_NUM_PARTITION_RELATION_ID - 1] = ObjectIdGetDatum(distributedTableId);
	values[ATTR_NUM_PARTITION_TYPE - 1] = CharGetDatum(partitionType);
	values[ATTR_NUM_PARTITION_KEY - 1] = PointerGetDatum(partitionKeyText);

	/* open the partition relation and insert new tuple */
	partitionRangeVar = makeRangeVar(METADATA_SCHEMA_NAME, PARTITION_TABLE_NAME, -1);
	partitionRelation = heap_openrv(partitionRangeVar, RowExclusiveLock);

	tupleDescriptor = RelationGetDescr(partitionRelation);
	heapTuple = heap_form_tuple(tupleDescriptor, values, isNulls);

	simple_heap_insert(partitionRelation, heapTuple);
	CatalogUpdateIndexes(partitionRelation, heapTuple);
	CommandCounterIncrement();

	/* close relation */
	relation_close(partitionRelation, RowExclusiveLock);
}


/*
 * InsertShardRow opens the shard metadata table and inserts a new row with
 * the given values into that table. Note that we allow the user to pass in
 * null min/max values.
 */
void
InsertShardRow(Oid distributedTableId, uint64 shardId, char shardStorage,
			   text *shardMinValue, text *shardMaxValue)
{
	Relation shardRelation = NULL;
	RangeVar *shardRangeVar = NULL;
	TupleDesc tupleDescriptor = NULL;
	HeapTuple heapTuple = NULL;
	Datum values[SHARD_TABLE_ATTRIBUTE_COUNT];
	bool isNulls[SHARD_TABLE_ATTRIBUTE_COUNT];

	/* form new shard tuple */
	memset(values, 0, sizeof(values));
	memset(isNulls, false, sizeof(isNulls));

	values[ATTR_NUM_SHARD_ID - 1] = Int64GetDatum(shardId);
	values[ATTR_NUM_SHARD_RELATION_ID - 1] = ObjectIdGetDatum(distributedTableId);
	values[ATTR_NUM_SHARD_STORAGE - 1] = CharGetDatum(shardStorage);

	/* check if shard min/max values are null */
	if (shardMinValue != NULL && shardMaxValue != NULL)
	{
		values[ATTR_NUM_SHARD_MIN_VALUE - 1] = PointerGetDatum(shardMinValue);
		values[ATTR_NUM_SHARD_MAX_VALUE - 1] = PointerGetDatum(shardMaxValue);
	}
	else
	{
		isNulls[ATTR_NUM_SHARD_MIN_VALUE - 1] = true;
		isNulls[ATTR_NUM_SHARD_MAX_VALUE - 1] = true;
	}

	/* open shard relation and insert new tuple */
	shardRangeVar = makeRangeVar(METADATA_SCHEMA_NAME, SHARD_TABLE_NAME, -1);
	shardRelation = heap_openrv(shardRangeVar, RowExclusiveLock);

	tupleDescriptor = RelationGetDescr(shardRelation);
	heapTuple = heap_form_tuple(tupleDescriptor, values, isNulls);

	simple_heap_insert(shardRelation, heapTuple);
	CatalogUpdateIndexes(shardRelation, heapTuple);
	CommandCounterIncrement();
	
	/* close relation */
	heap_close(shardRelation, RowExclusiveLock);
}


/*
 * InsertShardPlacementRow opens the shard placement metadata table and inserts
 * a row with the given values into the table.
 */
void
InsertShardPlacementRow(uint64 shardPlacementId, uint64 shardId,
						ShardState shardState, char *nodeName, uint32 nodePort)
{
	Relation shardPlacementRelation = NULL;
	RangeVar *shardPlacementRangeVar = NULL;
	TupleDesc tupleDescriptor = NULL;
	HeapTuple heapTuple = NULL;
	Datum values[SHARD_PLACEMENT_TABLE_ATTRIBUTE_COUNT];
	bool isNulls[SHARD_PLACEMENT_TABLE_ATTRIBUTE_COUNT];

	/* form new shard placement tuple */
	memset(values, 0, sizeof(values));
	memset(isNulls, false, sizeof(isNulls));

	values[ATTR_NUM_SHARD_PLACEMENT_ID - 1] = Int64GetDatum(shardPlacementId);
	values[ATTR_NUM_SHARD_PLACEMENT_SHARD_ID - 1] = Int64GetDatum(shardId);
	values[ATTR_NUM_SHARD_PLACEMENT_SHARD_STATE - 1] = UInt32GetDatum(shardState);
	values[ATTR_NUM_SHARD_PLACEMENT_NODE_NAME - 1] = CStringGetTextDatum(nodeName);
	values[ATTR_NUM_SHARD_PLACEMENT_NODE_PORT - 1] = UInt32GetDatum(nodePort);

	/* open shard placement relation and insert new tuple */
	shardPlacementRangeVar = makeRangeVar(METADATA_SCHEMA_NAME,
										  SHARD_PLACEMENT_TABLE_NAME, -1);
	shardPlacementRelation = heap_openrv(shardPlacementRangeVar, RowExclusiveLock);

	tupleDescriptor = RelationGetDescr(shardPlacementRelation);
	heapTuple = heap_form_tuple(tupleDescriptor, values, isNulls);

	simple_heap_insert(shardPlacementRelation, heapTuple);
	CatalogUpdateIndexes(shardPlacementRelation, heapTuple);
	CommandCounterIncrement();

	/* close relation */
	heap_close(shardPlacementRelation, RowExclusiveLock);
}


/*
 * DeleteShardPlacementRow removes the row corresponding to the provided shard
 * placement identifier, erroring out if it cannot find such a row.
 */
void
DeleteShardPlacementRow(uint64 shardPlacementId)
{
	RangeVar *heapRangeVar = NULL;
	RangeVar *indexRangeVar = NULL;
	Relation heapRelation = NULL;
	Relation indexRelation = NULL;
	IndexScanDesc indexScanDesc = NULL;
	const int scanKeyCount = 1;
	ScanKeyData scanKey[scanKeyCount];
	HeapTuple heapTuple = NULL;

	heapRangeVar = makeRangeVar(METADATA_SCHEMA_NAME, SHARD_PLACEMENT_TABLE_NAME, -1);
	indexRangeVar = makeRangeVar(METADATA_SCHEMA_NAME,
								 SHARD_PLACEMENT_PKEY_INDEX_NAME, -1);

	heapRelation = relation_openrv(heapRangeVar, RowExclusiveLock);
	indexRelation = relation_openrv(indexRangeVar, AccessShareLock);

	ScanKeyInit(&scanKey[0], 1, BTEqualStrategyNumber, F_INT8EQ,
				Int64GetDatum(shardPlacementId));

	indexScanDesc = index_beginscan(heapRelation, indexRelation, SnapshotNow,
									scanKeyCount, 0);
	index_rescan(indexScanDesc, scanKey, scanKeyCount, NULL, 0);

	heapTuple = index_getnext(indexScanDesc, ForwardScanDirection);
	if (HeapTupleIsValid(heapTuple))
	{
		simple_heap_delete(heapRelation, &heapTuple->t_self);
	}
	else
	{
		ereport(ERROR, (errmsg("could not find entry for shard placement " INT64_FORMAT,
							   shardPlacementId)));
	}

	index_endscan(indexScanDesc);
	index_close(indexRelation, AccessShareLock);
	relation_close(heapRelation, RowExclusiveLock);

	return;
}


/*
 * NextSequenceId allocates and returns a new unique id generated from the given
 * sequence name.
 */
uint64
NextSequenceId(char *sequenceName)
{
	RangeVar *sequenceRangeVar = makeRangeVar(METADATA_SCHEMA_NAME,
											  sequenceName, -1);
	bool failOk = false;
	Oid sequenceRelationId = RangeVarGetRelid(sequenceRangeVar, NoLock, failOk);
	Datum sequenceRelationIdDatum = ObjectIdGetDatum(sequenceRelationId);

	/* generate new and unique id from sequence */
	Datum sequenceIdDatum = DirectFunctionCall1(nextval_oid, sequenceRelationIdDatum);
	uint64 nextSequenceId = (uint64) DatumGetInt64(sequenceIdDatum);

	return nextSequenceId;
}
