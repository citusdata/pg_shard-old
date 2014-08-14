/*-------------------------------------------------------------------------
 *
 * test.c
 *		  Test wrapper functions for pg_shard
 *
 * Portions Copyright (c) 2014, Citus Data, Inc.
 *
 * IDENTIFICATION
 *		  test.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"
#include "fmgr.h"
#include "libpq-fe.h"
#include "pg_config.h"
#include "postgres_ext.h"

#include "connection.h"
#include "distribution_metadata.h"
#include "test.h"

#include <stddef.h>
#include <string.h>

#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/lsyscache.h"


/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(TestDistributionMetadata);
PG_FUNCTION_INFO_V1(TestPgShardConnection);


/*
 * TestDistributionMetadata prints all current shard and shard placement
 * configuration at INFO level for testing purposes.
 */
Datum
TestDistributionMetadata(PG_FUNCTION_ARGS)
{
	Oid distributedTableId = PG_GETARG_OID(0);

	Var *partitionColumn = PartitionColumn(distributedTableId);
	List *shardIntervalList = LoadShardIntervalList(distributedTableId);
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

	ereport(INFO, (errmsg("Found %d shards...", list_length(shardIntervalList))));

	foreach(cell, shardIntervalList)
	{
		ListCell *shardPlacementCell = NULL;
		ShardInterval *shardInterval = lfirst(cell);

		char *minValueStr = OutputFunctionCall(&outputFunctionInfo,
											   shardInterval->minValue);
		char *maxValueStr = OutputFunctionCall(&outputFunctionInfo,
											   shardInterval->maxValue);

		ereport(INFO, (errmsg("Shard Interval #" INT64_FORMAT, shardInterval->id)));
		ereport(INFO, (errmsg("\trelation:\t%s",
							  get_rel_name(shardInterval->relationId))));

		ereport(INFO, (errmsg("\tmin value:\t%s", minValueStr)));
		ereport(INFO, (errmsg("\tmax value:\t%s", maxValueStr)));

		shardPlacementList = LoadShardPlacementList(shardInterval->id);
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
 * TestPgShardConnection accepts three arguments: a hostname, port, and error
 * level. It connects to the host on the specified port and issues a RAISE at
 * the specified level.
 *
 * Intended for use in regression tests.
 */
Datum
TestPgShardConnection(PG_FUNCTION_ARGS)
{
	PGconn *connection = NULL;
	text *nodeText = PG_GETARG_TEXT_P(0);
	int32 nodePort = PG_GETARG_INT32(1);
	PGresult *result;

	char *nodeName = text_to_cstring(nodeText);

	connection = GetConnection(nodeName, nodePort);
	if (connection == NULL)
	{
		ereport(ERROR, (errmsg("could not connect to %s:%d", nodeName, nodePort)));
	}

	result = PQexec(connection, TEST_SQL);

	if (PQresultStatus(result) != PGRES_COMMAND_OK)
	{
		ReportRemoteError(connection, result);
	}

	PQclear(result);

	PG_RETURN_VOID();
}
