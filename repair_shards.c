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

#define DROP_TABLE_COMMAND "DROP %s TABLE IF EXISTS %s"


/* local function forward declarations */
static bool RepairShardPlacement(ShardPlacement *inactivePlacement,
								 ShardPlacement *healthyPlacement);
static List * RecreateTableDDLCommandList(Oid relationId, int64 shardId);
static bool CopyDataFromFinalizedPlacement(ShardPlacement *inactivePlacement,
										   ShardPlacement *healthyPlacement);

/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(repair_shard);


/*
 * repair_shard implements a user-facing UDF to repair all inactive placements within a
 * specified shard. If the shard has no healthy placements, this function throws an error.
 * If a particular placement cannot be repaired, this function prints a warning but will
 * not error out.
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
	 * By taking an exclusive lock on the shard, we both prevent all modifications
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
 * RepairShardPlacement repairs the specified inactive placement using data from the
 * specified healthy placement.
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
	DeleteShardPlacementRow(inactivePlacement->id);

	dataCopied = CopyDataFromFinalizedPlacement(inactivePlacement, healthyPlacement);
	if (!dataCopied)
	{
		/* return shard to inactive state since we were unable to repair */
		InsertShardPlacementRow(inactivePlacement->id, inactivePlacement->shardId,
								STATE_INACTIVE, inactivePlacement->nodeName,
								inactivePlacement->nodePort);

		return false;
	}

	InsertShardPlacementRow(inactivePlacement->id, inactivePlacement->shardId,
							STATE_FINALIZED, inactivePlacement->nodeName,
							inactivePlacement->nodePort);

	RESUME_INTERRUPTS();

	return true;
}


/*
 * RecreateTableDDLCommandList returns a list of DDL statements identical to that returned
 * by ExtendedDDLCommandList except that an extra "DROP TABLE" or "DROP FOREIGN TABLE"
 * statement is prepended to the list to facilitate total recreation of a placement.
 */
static List *
RecreateTableDDLCommandList(Oid relationId, int64 shardId)
{
	char *shardName = generate_shard_name(relationId, shardId);
	StringInfo workerDropQuery = makeStringInfo();
	List *ddlCommandList = TableDDLCommandList(relationId);
	char relationKind = get_rel_relkind(relationId);
	char *tableModifier = "";

	/* add shard identifier to DDL commands */
	ddlCommandList = ExtendedDDLCommandList(relationId, shardId,
											ddlCommandList);

	if (relationKind == RELKIND_FOREIGN_TABLE)
	{
		tableModifier = "FOREIGN";
	}
	else if (relationKind != RELKIND_RELATION)
	{
		ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
						errmsg("repair target is not a regular or foreign table")));
	}

	/* build drop table (or drop foreign table) command using shard identifier */
	appendStringInfo(workerDropQuery, DROP_TABLE_COMMAND, tableModifier, shardName);

	/* prepend drop table query to list of other DDL commands */
	ddlCommandList = lcons(workerDropQuery->data, ddlCommandList);

	return ddlCommandList;
}


/*
 * CopyDataFromFinalizedPlacement fills the specified inactive placement with data from
 * a finalized placement.
 */
static bool
CopyDataFromFinalizedPlacement(__attribute__ ((unused)) ShardPlacement *inactivePlacement,
							   __attribute__ ((unused)) ShardPlacement *healthyPlacement)
{
	/* TODO: Implement */
	return true;
}
