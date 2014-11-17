/*-------------------------------------------------------------------------
 *
 * repair_shards.c
 *		  Repair functionality for pg_shard.
 *
 * Portions Copyright (c) 2014, Citus Data, Inc.
 *
 * IDENTIFICATION
 *		  repair_shards.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "postgres_ext.h"

#include "connection.h"
#include "repair_shards.h"
#include "ddl_commands.h"
#include "distribution_metadata.h"
#include "pg_shard.h"
#include "ruleutils.h"

#include <string.h>

#include "access/heapam.h"
#include "catalog/pg_class.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "storage/lock.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"


/* local function forward declarations */
static ShardPlacement * SearchShardPlacementInList(List *shardPlacementList,
												   char *nodeName, int32 nodePort);
static List * RecreateTableDDLCommandList(Oid relationId, int64 shardId);
static bool CopyDataFromFinalizedPlacement(ShardPlacement *placementToRepair,
										   ShardPlacement *healthyPlacement,
										   Oid distributedTableId, int64 shardId);
static bool CopyDataFromTupleStoreToRelation(Tuplestorestate *tupleStore,
											 Relation relation);

/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(master_copy_shard_placement);
PG_FUNCTION_INFO_V1(copy_relation_from_node);


/*
 * master_copy_shard_placement implements a user-facing UDF to copy data from
 * a healthy (source) node to an inactive (target) node. To accomplish this it
 * entirely recreates the table structure before copying all data. During this
 * time all modifications are paused to the shard. After successful repair, the
 * inactive placement is marked healthy and modifications may continue. If the
 * repair fails at any point, this function throws an error, leaving the node
 * in an unhealthy state.
 */
Datum
master_copy_shard_placement(PG_FUNCTION_ARGS)
{
	int64 shardId = PG_GETARG_INT64(0);
	char *sourceNodeName = PG_GETARG_CSTRING(1);
	int32 sourceNodePort = PG_GETARG_INT32(2);
	char *targetNodeName = PG_GETARG_CSTRING(3);
	int32 targetNodePort = PG_GETARG_INT32(4);
	ShardInterval *shardInterval = LoadShardInterval(shardId);
	Oid distributedTableId = shardInterval->relationId;

	List *shardPlacementList = NIL;
	ShardPlacement *targetPlacement = NULL;
	ShardPlacement *sourcePlacement PG_USED_FOR_ASSERTS_ONLY = NULL;
	List *ddlCommandList = NIL;
	bool recreated = false;
	bool dataCopied = false;

	/*
	 * By taking an exclusive lock on the shard, we both stop all modifications
	 * (INSERT, UPDATE, or DELETE) and prevent concurrent repair operations from
	 * being able to operate on this shard.
	 */
	LockShard(shardId, ExclusiveLock);

	shardPlacementList = LoadShardPlacementList(shardId);
	targetPlacement = SearchShardPlacementInList(shardPlacementList, targetNodeName,
												 targetNodePort);
	sourcePlacement = SearchShardPlacementInList(shardPlacementList, sourceNodeName,
												 sourceNodePort);

	Assert(targetPlacement->shardState == STATE_INACTIVE);
	Assert(sourcePlacement->shardState == STATE_FINALIZED);

	/* retrieve the DDL commands for the table and run them */
	ddlCommandList = RecreateTableDDLCommandList(distributedTableId, shardId);

	recreated = ExecuteRemoteCommandList(targetPlacement->nodeName,
										 targetPlacement->nodePort,
										 ddlCommandList);
	if (!recreated)
	{
		ereport(ERROR, (errmsg("could not recreate table to receive placement data")));
	}

	HOLD_INTERRUPTS();
	dataCopied = CopyDataFromFinalizedPlacement(sourcePlacement, targetPlacement,
												distributedTableId, shardId);
	if (!dataCopied)
	{
		ereport(ERROR, (errmsg("failed to copy placement data")));
	}

	/* the placement is repaired, so return to finalized state */
	DeleteShardPlacementRow(targetPlacement->id);
	InsertShardPlacementRow(targetPlacement->id, targetPlacement->shardId,
							STATE_FINALIZED, targetPlacement->nodeName,
							targetPlacement->nodePort);

	RESUME_INTERRUPTS();

	PG_RETURN_VOID();
}


/*
 * repair_shard_placement implements a internal UDF to copy a table's data from
 * a healthy placement into a receiving table on an unhealthy placement. This
 * function returns a boolean reflecting success or failure.
 */
Datum
copy_relation_from_node(PG_FUNCTION_ARGS)
{
	Oid distributedTableId = PG_GETARG_OID(0);
	char *nodeName = PG_GETARG_CSTRING(1);
	int32 nodePort = PG_GETARG_INT32(2);

	Relation distributedTable = heap_open(distributedTableId, RowExclusiveLock);
	char *relationName = RelationGetRelationName(distributedTable);
	ShardPlacement *placement = (ShardPlacement *) palloc0(sizeof(ShardPlacement));
	Task *task = (Task *) palloc0(sizeof(Task));
	StringInfo selectAllQuery = makeStringInfo();

	TupleDesc tupleDescriptor = RelationGetDescr(distributedTable);
	Tuplestorestate *tupleStore = tuplestore_begin_heap(false, false, work_mem);
	bool fetchSuccessful = false;
	bool loadSuccessful = false;

	appendStringInfo(selectAllQuery, SELECT_ALL_QUERY, quote_identifier(relationName));

	placement->nodeName = nodeName;
	placement->nodePort = nodePort;

	task->queryString = selectAllQuery;
	task->taskPlacementList = list_make1(placement);

	fetchSuccessful = ExecuteTaskAndStoreResults(task, tupleDescriptor, tupleStore);
	if (!fetchSuccessful)
	{
		ereport(WARNING, (errmsg("could not receive query results")));
		PG_RETURN_BOOL(false);
	}

	loadSuccessful = CopyDataFromTupleStoreToRelation(tupleStore, distributedTable);
	if (!loadSuccessful)
	{
		ereport(WARNING, (errmsg("could not load query results")));
		PG_RETURN_BOOL(false);
	}

	tuplestore_end(tupleStore);

	heap_close(distributedTable, RowExclusiveLock);

	PG_RETURN_BOOL(true);
}


/*
 * SearchShardPlacementInList searches a provided list for a shard placement
 * with the specified node name and port. This function throws an error if no
 * such placement exists in the provided list.
 */
static ShardPlacement *
SearchShardPlacementInList(List *shardPlacementList, char *nodeName, int32 nodePort)
{
	ListCell *shardPlacementCell = NULL;
	ShardPlacement *matchingPlacement = NULL;

	foreach(shardPlacementCell, shardPlacementList)
	{
		ShardPlacement *shardPlacement = lfirst(shardPlacementCell);

		if (strncmp(nodeName, shardPlacement->nodeName, MAX_NODE_LENGTH) &&
			nodePort == shardPlacement->nodePort)
		{
			matchingPlacement = shardPlacement;
			break;
		}
	}

	if (matchingPlacement == NULL)
	{
		ereport(ERROR, (errmsg("could not find placement matching %s:%d", nodeName,
							   nodePort)));
	}

	return matchingPlacement;
}


/*
 * RecreateTableDDLCommandList returns a list of DDL statements similar to that
 * returned by ExtendedDDLCommandList except that the list begins with a "DROP
 * TABLE" or "DROP FOREIGN TABLE" statement to facilitate total recreation of a
 * placement.
 */
static List *
RecreateTableDDLCommandList(Oid relationId, int64 shardId)
{
	char *relationName = get_rel_name(relationId);
	const char *shardName = NULL;
	StringInfo extendedDropCommand = makeStringInfo();
	List *createCommandList = NIL;
	List *extendedCreateCommandList = NIL;
	List *extendedDropCommandList = NIL;
	List *extendedRecreateCommandList = NIL;
	char relationKind = get_rel_relkind(relationId);

	AppendShardIdToName(&relationName, shardId);
	shardName = quote_identifier(relationName);

	/* build appropriate DROP command based on relation kind */
	if (relationKind == RELKIND_RELATION)
	{
		appendStringInfo(extendedDropCommand, DROP_REGULAR_TABLE_COMMAND,
						 quote_identifier(shardName));
	}
	else if (relationKind == RELKIND_FOREIGN_TABLE)
	{
		appendStringInfo(extendedDropCommand, DROP_FOREIGN_TABLE_COMMAND,
						 quote_identifier(shardName));
	}
	else
	{
		ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
						errmsg("repair target is not a regular or foreign table")));
	}

	extendedDropCommandList = list_make1(extendedDropCommand->data);

	createCommandList = TableDDLCommandList(relationId);
	extendedCreateCommandList = ExtendedDDLCommandList(relationId, shardId,
													   createCommandList);

	extendedRecreateCommandList = list_union(extendedDropCommandList,
											 extendedCreateCommandList);

	return extendedRecreateCommandList;
}


/*
 * CopyDataFromFinalizedPlacement connects to an unhealthy placement and
 * directs it to copy the specified shard from a certain healthy placement.
 * This function assumes that the unhealthy placement already has a schema
 * in place to receive rows from the healthy placement. This function returns
 * a boolean indicating success or failure.
 */
static bool
CopyDataFromFinalizedPlacement(ShardPlacement *placementToRepair,
							   ShardPlacement *healthyPlacement,
							   Oid distributedTableId, int64 shardId)
{
	char *relationName = get_rel_name(distributedTableId);
	const char *shardName = NULL;
	char *nodeName = placementToRepair->nodeName;
	int32 nodePort = placementToRepair->nodePort;
	StringInfo copyRelationQuery = makeStringInfo();

	PGconn *connection = NULL;
	PGresult *result = NULL;
	char *copySuccessfulString = NULL;
	bool copySuccessful = false;
	bool responseParsedSuccessfully = false;

	AppendShardIdToName(&relationName, shardId);
	shardName = quote_identifier(relationName);

	connection = GetConnection(nodeName, nodePort);
	if (connection == NULL)
	{
		ereport(WARNING, (errmsg("could not connect to %s:%d", nodeName, nodePort)));

		return false;
	}

	appendStringInfo(copyRelationQuery, COPY_RELATION_QUERY,
					 quote_literal_cstr(shardName),
					 quote_literal_cstr(healthyPlacement->nodeName),
					 healthyPlacement->nodePort);


	result = PQexec(connection, copyRelationQuery->data);
	if (PQresultStatus(result) != PGRES_TUPLES_OK)
	{
		ReportRemoteError(connection, result);
		PQclear(result);

		return false;
	}

	Assert(PQntuples(result) == 1);
	Assert(PQnfields(result) == 1);

	copySuccessfulString = PQgetvalue(result, 0, 0);
	responseParsedSuccessfully = parse_bool(copySuccessfulString, &copySuccessful);

	Assert(responseParsedSuccessfully);

	PQclear(result);

	return true;
}


/*
 * CopyDataFromTupleStoreToRelation loads a specified relation with all tuples
 * stored in the provided tuplestore. This function assumes the relation's
 * layout (TupleDesc) exactly matches that of the provided tuplestore. This
 * function returns a boolean indicating success or failure.
 */
static bool
CopyDataFromTupleStoreToRelation(Tuplestorestate *tupleStore, Relation relation)
{
	TupleDesc tupleDescriptor = RelationGetDescr(relation);
	TupleTableSlot *tupleTableSlot = MakeSingleTupleTableSlot(tupleDescriptor);
	bool copySuccessful = false;

	for (;;)
	{
		HeapTuple tupleToInsert = NULL;
		Oid insertedOid = InvalidOid;
		bool nextTuple = tuplestore_gettupleslot(tupleStore, true, false, tupleTableSlot);
		if (!nextTuple)
		{
			copySuccessful = true;
			break;
		}

		tupleToInsert = ExecMaterializeSlot(tupleTableSlot);

		insertedOid = simple_heap_insert(relation, tupleToInsert);
		if (insertedOid == InvalidOid)
		{
			copySuccessful = false;
			break;
		}

		ExecClearTuple(tupleTableSlot);
	}

	ExecDropSingleTupleTableSlot(tupleTableSlot);

	return copySuccessful;
}
