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
#include "pg_config.h"
#include "postgres_ext.h"

#include "repair_shards.h"
#include "ddl_commands.h"
#include "distribution_metadata.h"
#include "ruleutils.h"

#include <string.h>

#include "catalog/pg_class.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "storage/lock.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/lsyscache.h"


/* local function forward declarations */
static bool RepairShardPlacement(ShardPlacement *inactivePlacement,
								 ShardPlacement *healthyPlacement);
static List * RecreateTableDDLCommandList(Oid relationId, int64 shardId);
static bool CopyDataFromFinalizedPlacement(ShardPlacement *inactivePlacement,
										   ShardPlacement *healthyPlacement);

/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(repair_shard);


/*
 * repair_shard implements a user-facing UDF to repair all inactive placements
 * within a specified shard. If the shard has no healthy placements, this
 * function throws an error. If a particular placement cannot be repaired, this
 * function prints a warning but will not error out.
 */
Datum
repair_shard(PG_FUNCTION_ARGS)
{
	int64 shardId = PG_GETARG_INT64(0);
	List *shardPlacementList = LoadShardPlacementList(shardId);
	ListCell *shardPlacementCell = NULL;
	ShardPlacement *healthyPlacement = NULL;

	foreach(shardPlacementCell, shardPlacementList)
	{
		ShardPlacement *shardPlacement = (ShardPlacement *) lfirst(shardPlacementCell);

		if (shardPlacement->shardState == STATE_FINALIZED)
		{
			healthyPlacement = shardPlacement;
			break;
		}
	}

	if (healthyPlacement == NULL)
	{
		ereport(ERROR, (errmsg("could not find healthy placement for shard " INT64_FORMAT,
							   shardId)));
	}

	/*
	 * By taking an exclusive lock on the shard, we both stop all modifications
	 * (INSERT, UPDATE, or DELETE) and prevent concurrent repair operations from
	 * being able to operate on this shard.
	 */
	LockShard(shardId, ExclusiveLock);

	foreach(shardPlacementCell, shardPlacementList)
	{
		ShardPlacement *shardPlacement = (ShardPlacement *) lfirst(shardPlacementCell);

		if (shardPlacement->shardState == STATE_INACTIVE)
		{
			bool repairSuccess = RepairShardPlacement(shardPlacement, healthyPlacement);

			if (!repairSuccess)
			{
				ereport(WARNING, (errmsg("could not repair placement " INT64_FORMAT,
										 shardPlacement->id)));
			}
		}
	}

	PG_RETURN_VOID();
}


/*
 * RepairShardPlacement repairs the specified inactive placement using data from
 * the specified healthy placement.
 */
static bool
RepairShardPlacement(ShardPlacement *inactivePlacement, ShardPlacement *healthyPlacement)
{
	int64 shardId = inactivePlacement->shardId;
	ShardInterval *shardInterval = LoadShardInterval(shardId);
	Oid distributedTableId = shardInterval->relationId;

	List *ddlCommandList = NIL;
	bool recreated = false;
	bool dataCopied = false;

	Assert(shardId == healthyPlacement->shardId);

	/* retrieve the DDL commands for the table and run them */
	ddlCommandList = RecreateTableDDLCommandList(distributedTableId, shardId);

	recreated = ExecuteRemoteCommandList(inactivePlacement->nodeName,
										 inactivePlacement->nodePort,
										 ddlCommandList);

	if (!recreated)
	{
		return false;
	}

	HOLD_INTERRUPTS();

	dataCopied = CopyDataFromFinalizedPlacement(inactivePlacement, healthyPlacement);
	if (!dataCopied)
	{
		return false;
	}

	/* the placement is repaired, so return to finalized state */
	DeleteShardPlacementRow(inactivePlacement->id);
	InsertShardPlacementRow(inactivePlacement->id, inactivePlacement->shardId,
							STATE_FINALIZED, inactivePlacement->nodeName,
							inactivePlacement->nodePort);

	RESUME_INTERRUPTS();

	return true;
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
	char *shardName = generate_shard_name(relationId, shardId);
	StringInfo extendedDropCommand = makeStringInfo();
	List *createCommandList = NIL;
	List *extendedCreateCommandList = NIL;
	List *extendedDropCommandList = NIL;
	List *extendedRecreateCommandList = NIL;
	char relationKind = get_rel_relkind(relationId);

	/* build appropriate DROP command based on relation kind */
	if (relationKind == RELKIND_RELATION)
	{
		appendStringInfo(extendedDropCommand, DROP_REGULAR_TABLE_COMMAND, shardName);
	}
	else if (relationKind == RELKIND_FOREIGN_TABLE)
	{
		appendStringInfo(extendedDropCommand, DROP_FOREIGN_TABLE_COMMAND, shardName);
	}
	else if (relationKind != RELKIND_RELATION)
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
 * CopyDataFromFinalizedPlacement fills the specified inactive placement with
 * data from a finalized placement.
 */
static bool
CopyDataFromFinalizedPlacement(__attribute__ ((unused)) ShardPlacement *inactivePlacement,
							   __attribute__ ((unused)) ShardPlacement *healthyPlacement)
{
	/* TODO: Implement */
	return true;
}
