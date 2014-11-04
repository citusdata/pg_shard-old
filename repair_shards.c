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
static void AcquireRepairLock();
static List * RecreateTableDDLCommandList(Oid relationId, int64 shardId);
static bool CopyDataFromFinalizedPlacement(ShardPlacement *shardPlacement, int64 shardId);

/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(repair_shard_placement);


/*
 * repair_shard_placement finds a shard placement by ID and attempts to repair it if it
 * is not in a healthy state.
 */
Datum
repair_shard_placement(PG_FUNCTION_ARGS)
{
	int64 shardPlacementId = PG_GETARG_INT64(0);
	ShardPlacement *shardPlacement = LoadShardPlacement(shardPlacementId);
	int64 shardId = shardPlacement->shardId;
	ShardInterval *shardInterval = LoadShardInterval(shardId);
	Oid distributedTableId = shardInterval->relationId;

	List *ddlCommandList = NIL;
	bool recreated = false;
	bool dataCopied = false;

	if (shardPlacement->shardState != STATE_INACTIVE)
	{
		ereport(ERROR, (errmsg("can only repair placements in inactive state")));
	}

	/*
	 * By taking an exclusive lock on the shard, we both prevent all modifications
	 * (INSERT, UPDATE, or DELETE) and prevent concurrent repair operations from
	 * being able to operate on this shard.
	 */
	LockShard(shardId, ExclusiveLock);

	/* retrieve the DDL commands for the table and run them */
	ddlCommandList = RecreateTableDDLCommandList(distributedTableId, shardId);

	recreated = ExecuteRemoteCommandList(shardPlacement->nodeName,
										 shardPlacement->nodePort,
										 ddlCommandList);

	if (!recreated)
	{
		ereport(ERROR, (errmsg("failed to recreate shard on placement")));
	}

	HOLD_INTERRUPTS();
	DeleteShardPlacementRow(shardPlacementId);

	dataCopied = CopyDataFromFinalizedPlacement(shardPlacement, shardId);
	if (!dataCopied)
	{
		/* return shard to inactive state since we were unable to repair */
		InsertShardPlacementRow(shardPlacement->id, shardPlacement->shardId,
								STATE_INACTIVE, shardPlacement->nodeName,
								shardPlacement->nodePort);

		ereport(ERROR, (errmsg("failed to copy data to recreated shard")));
	}

	InsertShardPlacementRow(shardPlacement->id, shardPlacement->shardId, STATE_FINALIZED,
							shardPlacement->nodeName, shardPlacement->nodePort);

	RESUME_INTERRUPTS();

	PG_RETURN_VOID();
}


/*
 * AcquireRepairLock returns after acquiring the node-wide repair lock. This function will
 * raise an error if the lock is already held by a repair operation running in another
 * session.
 */
static void
AcquireRepairLock()
{
	LOCKTAG lockTag;
	memset(&lockTag, 0, sizeof(LOCKTAG));
	bool sessionLock = false;	/* we want a transaction lock */
	bool dontWait = true;		/* don't block */
	LockAcquireResult result = LOCKACQUIRE_NOT_AVAIL;

	/* pass two as the fourth lock field to avoid conflict with other locks */
	SET_LOCKTAG_ADVISORY(lockTag, MyDatabaseId, 0, 0, 2);

	result = LockAcquire(&lockTag, ExclusiveLock, sessionLock, dontWait);

	if (result == LOCKACQUIRE_NOT_AVAIL)
	{
		ereport(ERROR, (errmsg("concurrent repair operations already underway")));
	}
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
 * CopyDataFromFinalizedPlacement fills the specified placement with the healthy data from
 * a finalized placement within the specified shard. If the specified shard has no such
 * finalized placements, this function errors out.
 */
static bool
CopyDataFromFinalizedPlacement(__attribute__ ((unused)) ShardPlacement *shardPlacement,
							   __attribute__ ((unused)) int64 shardId)
{
	/* TODO: Implement */
	return true;
}
