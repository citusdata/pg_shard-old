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
#include "catalog/pg_type.h"
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
	memset(&outputFunctionInfo, 0, sizeof(outputFunctionInfo));
	Oid outputFunctionId = InvalidOid;
	bool isVarlena = false;

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
		int64 *shardId = NULL;

		shardId = (int64 *) lfirst(cell);
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
		ShardPlacement *shardPlacement =
				TupleToShardPlacement(heapTuple, tupleDescriptor);
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
						errmsg("partition column \"%s\" not found",
							   columnName)));
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

	Datum idDatum =
			heap_getattr(heapTuple, ATTR_NUM_SHARD_PLACEMENT_ID,
						 tupleDescriptor, &isNull);
	Datum shardIdDatum =
			heap_getattr(heapTuple, ATTR_NUM_SHARD_PLACEMENT_SHARD_ID,
						 tupleDescriptor, &isNull);
	Datum nodeNameDatum =
			heap_getattr(heapTuple, ATTR_NUM_SHARD_PLACEMENT_NODE_NAME,
						 tupleDescriptor, &isNull);
	Datum nodePortDatum =
			heap_getattr(heapTuple, ATTR_NUM_SHARD_PLACEMENT_NODE_PORT,
						 tupleDescriptor, &isNull);

	shardPlacement = palloc0(sizeof(ShardPlacement));
	shardPlacement->id = DatumGetInt64(idDatum);
	shardPlacement->shardId = DatumGetInt64(shardIdDatum);
	shardPlacement->nodeName = TextDatumGetCString(nodeNameDatum);
	shardPlacement->nodePort = DatumGetInt32(nodePortDatum);

	return shardPlacement;
}
