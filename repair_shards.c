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

#include <string.h>

#include "catalog/pg_class.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "storage/lock.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/lsyscache.h"


/* local function forward declarations */
static ShardPlacement * SearchShardPlacementInList(List *shardPlacementList,
												   char *nodeName, int32 nodePort);
static List * RecreateTableDDLCommandList(Oid relationId, int64 shardId);

/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(master_copy_shard_placement);


/*
 * master_copy_shard_placement implements a user-facing UDF to repair a
 * specific inactive placement using data from a specific healthy placement.
 * If the repair fails at any point, this function throws an error.
 */
Datum
master_copy_shard_placement(PG_FUNCTION_ARGS)
{
	char *unhealthyNodeName = PG_GETARG_CSTRING(0);
	int32 unhealthyNodePort = PG_GETARG_INT32(1);
	char *healthyNodeName = PG_GETARG_CSTRING(2);
	int32 healthyNodePort = PG_GETARG_INT32(3);
	int64 shardId = PG_GETARG_INT64(4);
	ShardInterval *shardInterval = LoadShardInterval(shardId);
	Oid distributedTableId = shardInterval->relationId;

	List *shardPlacements = NIL;
	ShardPlacement *placementToRepair = NULL;
	ShardPlacement *healthyPlacement = NULL;
	List *ddlCommandList = NIL;
	bool recreated = false;

	/*
	 * By taking an exclusive lock on the shard, we both stop all modifications
	 * (INSERT, UPDATE, or DELETE) and prevent concurrent repair operations from
	 * being able to operate on this shard.
	 */
	LockShard(shardId, ExclusiveLock);

	shardPlacements = LoadShardPlacementList(shardId);
	placementToRepair = SearchShardPlacementInList(shardPlacements, unhealthyNodeName,
												   unhealthyNodePort);
	healthyPlacement = SearchShardPlacementInList(shardPlacements, healthyNodeName,
												  healthyNodePort);

	Assert(placementToRepair->shardState == STATE_INACTIVE);
	Assert(healthyPlacement->shardState == STATE_FINALIZED);

	/* retrieve the DDL commands for the table and run them */
	ddlCommandList = RecreateTableDDLCommandList(distributedTableId, shardId);

	recreated = ExecuteRemoteCommandList(placementToRepair->nodeName,
										 placementToRepair->nodePort,
										 ddlCommandList);
	if (!recreated)
	{
		ereport(ERROR, (errmsg("could not recreate table to receive placement data")));
	}

	HOLD_INTERRUPTS();

	/* TODO: implement row copying functionality */
	ereport(ERROR, (errmsg("shard placement repair not fully implemented")));

	/* the placement is repaired, so return to finalized state */
	DeleteShardPlacementRow(placementToRepair->id);
	InsertShardPlacementRow(placementToRepair->id, placementToRepair->shardId,
							STATE_FINALIZED, placementToRepair->nodeName,
							placementToRepair->nodePort);

	RESUME_INTERRUPTS();

	PG_RETURN_VOID();
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
	shardName = quote_identifier(shardName);

	/* build appropriate DROP command based on relation kind */
	if (relationKind == RELKIND_RELATION)
	{
		appendStringInfo(extendedDropCommand, DROP_REGULAR_TABLE_COMMAND, shardName);
	}
	else if (relationKind == RELKIND_FOREIGN_TABLE)
	{
		appendStringInfo(extendedDropCommand, DROP_FOREIGN_TABLE_COMMAND, shardName);
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

