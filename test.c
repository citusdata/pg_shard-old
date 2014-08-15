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

#include "catalog/pg_type.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/lsyscache.h"


/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(PopulateTempTable);
PG_FUNCTION_INFO_V1(CountTempTable);
PG_FUNCTION_INFO_V1(GetAndPurgeConnection);
PG_FUNCTION_INFO_V1(TestDistributionMetadata);


/* local function forward declarations */
static PGconn * GetConnectionOrRaiseError(text *nodeText, int32 nodePort);
static Datum ExtractIntegerDatum(char *input);


/*
 * PopulateTempTable connects to a specified host on a specified port and
 * creates a temporary table with 100 rows. Because the table is temporary, it
 * will be visible if a connection is reused but not if a new connection is
 * opened to the node.
 */
Datum
PopulateTempTable(PG_FUNCTION_ARGS)
{
	text *nodeText = PG_GETARG_TEXT_P(0);
	int32 nodePort = PG_GETARG_INT32(1);
	PGconn *connection = GetConnectionOrRaiseError(nodeText, nodePort);
	PGresult *result = PQexec(connection, POPULATE_TEMP_TABLE);

	if (PQresultStatus(result) != PGRES_COMMAND_OK)
	{
		ReportRemoteError(connection, result);
	}

	PQclear(result);

	PG_RETURN_VOID();
}


/*
 * CountTempTable just returns the integer count of rows in the table created
 * by PopulateTempTable. If no such table exists, this function emits a warning
 * and returns 0.
 */
Datum
CountTempTable(PG_FUNCTION_ARGS)
{
	text *nodeText = PG_GETARG_TEXT_P(0);
	int32 nodePort = PG_GETARG_INT32(1);
	PGconn *connection = GetConnectionOrRaiseError(nodeText, nodePort);
	PGresult *result = PQexec(connection, COUNT_TEMP_TABLE);
	Datum count = 0;

	if (PQresultStatus(result) != PGRES_TUPLES_OK)
	{
		ReportRemoteError(connection, result);
		count = Int32GetDatum(0);
	}
	else
	{
		char *countText = PQgetvalue(result, 0, 0);
		count = ExtractIntegerDatum(countText);
	}

	PQclear(result);

	PG_RETURN_DATUM(count);
}


/*
 * GetAndPurgeConnection first gets a connection using the provided hostname and
 * port before immediately passing that connection to PurgeConnection. Simply a
 * wrapper around PurgeConnection that uses hostname/port rather than PGconn.
 */
Datum
GetAndPurgeConnection(PG_FUNCTION_ARGS)
{
	text *nodeText = PG_GETARG_TEXT_P(0);
	int32 nodePort = PG_GETARG_INT32(1);
	PGconn *connection = GetConnectionOrRaiseError(nodeText, nodePort);

	PurgeConnection(connection);

	PG_RETURN_VOID();
}


/*
 * GetConnectionOrRaiseError just wraps GetConnection to throw an error if no
 * connection can be established.
 */
static PGconn *
GetConnectionOrRaiseError(text *nodeText, int32 nodePort)
{
	char *nodeName = text_to_cstring(nodeText);

	PGconn *connection = GetConnection(nodeName, nodePort);
	if (connection == NULL)
	{
		ereport(ERROR, (errmsg("could not connect to %s:%d", nodeName, nodePort)));
	}

	return connection;
}


/*
 * ExtractIntegerDatum transforms an integer in textual form into a Datum.
 */
static Datum
ExtractIntegerDatum(char *input)
{
	Oid typiofunc = InvalidOid;
	Oid typioparam = InvalidOid;
	Datum intDatum = 0;
	FmgrInfo fmgrInfo;
	memset(&fmgrInfo, 0, sizeof(fmgrInfo));

	getTypeInputInfo(INT4OID, &typiofunc, &typioparam);
	fmgr_info(typiofunc, &fmgrInfo);

	intDatum = InputFunctionCall(&fmgrInfo, input, typiofunc, -1);

	return intDatum;
}

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

