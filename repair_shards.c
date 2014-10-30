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
#include "libpq-fe.h"
#include "miscadmin.h"
#include "pg_config.h"
#include "postgres_ext.h"

#include "repair_shards.h"
#include "connection.h"
#include "ddl_commands.h"
#include "distribution_metadata.h"
#include "ruleutils.h"

#include <string.h>

#include "access/htup_details.h"
#include "access/htup.h"
#include "catalog/pg_class.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "postmaster/postmaster.h"
#include "storage/lock.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#define DROP_TABLE_COMMAND "DROP TABLE IF EXISTS %s"
#define CHANGE_PLACEMENT_COMMAND "SELECT change_placement_state("INT64_FORMAT", %d)"


/* local function forward declarations */
static void AcquireRepairLock();
static List * RecreateTableDDLCommandList(Oid relationId, int64 shardId);
static ShardState ChangeAndCommitPlacementState(int64 placementId, ShardState newState);
static bool CopyDataFromFinalizedPlacement(ShardPlacement *shardPlacement, int64 shardId);

/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(repair_shard_placement);
PG_FUNCTION_INFO_V1(change_placement_state);


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

	AcquireRepairLock();

	/* retrieve the DDL commands for the table and run them */
	ddlCommandList = RecreateTableDDLCommandList(distributedTableId, shardId);

	recreated = WorkerCreateShard(shardPlacement->nodeName, shardPlacement->nodePort,
								  ddlCommandList);
	if (!recreated)
	{
		ereport(ERROR, (errmsg("failed to recreate shard on placement")));
	}

	/* Prevent UPDATE/DELETE races and interrupts */
	LockShard(shardId, ShareLock);
	HOLD_INTERRUPTS();

	ChangeAndCommitPlacementState(shardPlacement->id, STATE_NO_MODIFY);

	dataCopied = CopyDataFromFinalizedPlacement(shardPlacement, shardId);
	if (!dataCopied)
	{
		/* return shard to inactive state since we were unable to repair */
		ChangeAndCommitPlacementState(shardPlacement->id, STATE_INACTIVE);

		ereport(ERROR, (errmsg("failed to copy data to recreated shard")));
	}

	ChangeAndCommitPlacementState(shardPlacement->id, STATE_FINALIZED);

	RESUME_INTERRUPTS();

	PG_RETURN_VOID();
}


/*
 * change_placement_state finds a shard placement by ID and updates its state with the
 * provided value.
 */
Datum
change_placement_state(PG_FUNCTION_ARGS)
{
	int64 shardPlacementId = PG_GETARG_INT64(0);
	ShardState newState = (ShardState) PG_GETARG_INT32(1);
	ShardPlacement *shardPlacement = LoadShardPlacement(shardPlacementId);
	ShardState prevState = shardPlacement->shardState;

	DeleteShardPlacementRow(shardPlacementId);
	InsertShardPlacementRow(shardPlacement->id, shardPlacement->shardId, newState,
							shardPlacement->nodeName, shardPlacement->nodePort);

	PG_RETURN_INT32(prevState);
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
 * by ExtendedDDLCommandList except that an extra "DROP TABLE" statement is prepended to
 * the list to facilitate total recreation of a placement.
 */
static List *
RecreateTableDDLCommandList(Oid relationId, int64 shardId)
{
	char *shardName = generate_shard_name(relationId, shardId);
	StringInfo workerDropQuery = makeStringInfo();
	List *ddlCommandList = TableDDLCommandList(relationId);

	/* add shard identifier to DDL commands */
	ddlCommandList = ExtendedDDLCommandList(relationId, shardId,
											ddlCommandList);

	/* build drop table command using shard identifier */
	/* TODO: handle foreign tables */
	appendStringInfo(workerDropQuery, DROP_TABLE_COMMAND, shardName);

	/* prepend drop table query to list of other DDL commands */
	ddlCommandList = lcons(workerDropQuery->data, ddlCommandList);

	return ddlCommandList;
}


/*
 * ChangeAndCommitPlacementState updates the specified placement to have a new state in
 * such a way that this change is visible to other sessions before the calling session's
 * top level transaction is committed. This is accomplished by executing a "remote"
 * command on localhost using libpq, which commits immediately.
 *
 */
static ShardState
ChangeAndCommitPlacementState(int64 placementId, ShardState newState)
{
	PGconn *loopback = GetConnection("localhost", PostPortNumber);
	StringInfo changePlacementQuery = makeStringInfo();
	PGresult *result = NULL;
	char *stateString = "";
	ShardState prevState = STATE_INVALID_FIRST;

	if (loopback == NULL)
	{
		ereport(ERROR, (errmsg("could not establish connection")));
	}

	appendStringInfo(changePlacementQuery, CHANGE_PLACEMENT_COMMAND,
					 placementId, (int32) newState);

	result = PQexec(loopback, changePlacementQuery->data);

	if (PQresultStatus(result) != PGRES_TUPLES_OK)
	{
		ReportRemoteError(loopback, result);
		PQclear(result);

		ereport(ERROR, (errmsg("could not modify placement state")));
	}

	Assert(PQntuples(result) == 1);
	Assert(PQnfields(result) == 1);

	stateString = PQgetvalue(result, 0, 0);

	prevState = (ShardState) pg_atoi(stateString, sizeof(int32), 0);
	PQclear(result);

	return prevState;
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
