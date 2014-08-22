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
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/lsyscache.h"


/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(PopulateTempTable);
PG_FUNCTION_INFO_V1(CountTempTable);
PG_FUNCTION_INFO_V1(GetAndPurgeConnection);
PG_FUNCTION_INFO_V1(TestDistributionMetadata);
PG_FUNCTION_INFO_V1(LoadShardIdArray);
PG_FUNCTION_INFO_V1(LoadShardIntervalArray);
PG_FUNCTION_INFO_V1(LoadShardPlacementArray);


/* local function forward declarations */
static PGconn * GetConnectionOrRaiseError(text *nodeText, int32 nodePort);
static Datum ExtractIntegerDatum(char *input);
static ArrayType * DatumArrayToArrayType(Datum *datumArray, int datumCount,
										 Oid datumTypeId);


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
	getTypeOutputInfo(INT4OID, &outputFunctionId, &isVarlena);
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
 * LoadShardIdArray returns the shard identifiers for a particular distributed
 * table as a bigint array.
 */
Datum
LoadShardIdArray(PG_FUNCTION_ARGS)
{
	Oid distributedTableId = PG_GETARG_OID(0);
	ArrayType *shardIdArrayType = NULL;
	List *shardList = LoadShardList(distributedTableId);
	ListCell *shardCell = NULL;
	int datumCount = list_length(shardList);
	int datumIndex = 0;
	Datum *shardIdDatums = palloc0(datumCount * sizeof(Datum));
	Oid datumTypeId = INT8OID;

	foreach(shardCell, shardList)
	{
		int64 *shardId = (int64 *) lfirst(shardCell);
		Datum shardIdDatum = Int64GetDatum(*shardId);

		shardIdDatums[datumIndex] = shardIdDatum;
		datumIndex++;
	}

	shardIdArrayType = DatumArrayToArrayType(shardIdDatums, datumCount, datumTypeId);

	pfree(shardIdDatums);

	PG_RETURN_ARRAYTYPE_P(shardIdArrayType);
}


/*
 * LoadShardIntervalArray loads a shard interval using a provided identifier and
 * returns a two-element array consisting of the min and max values contained in
 * that shard interval (currently always integer values). If no such interval
 * can be found, this function raises an error instead.
 */
Datum
LoadShardIntervalArray(PG_FUNCTION_ARGS)
{
	int64 shardId = PG_GETARG_INT64(0);
	ArrayType *shardIntervalArrayType = NULL;
	ShardInterval *shardInterval = LoadShardInterval(shardId);
	Datum shardIntervalArray[] = { shardInterval->minValue, shardInterval->maxValue };

	/* for now we expect value type to always be integer (hash output) */
	Assert(shardInterval->valueTypeId == INT4OID);

	shardIntervalArrayType = DatumArrayToArrayType(shardIntervalArray, 2,
												   shardInterval->valueTypeId);

	PG_RETURN_ARRAYTYPE_P(shardIntervalArrayType);
}


/*
 * LoadShardPlacementArray loads a shard interval using a provided identifier
 * and returns an array of strings containing the node name and port for each
 * placement of the specified shard interval. If no such shard interval can be
 * found, this function raises an error instead.
 */
Datum
LoadShardPlacementArray(PG_FUNCTION_ARGS)
{
	int64 shardId = PG_GETARG_INT64(0);
	ArrayType *placementArrayType = NULL;
	List *placementList = LoadShardPlacementList(shardId);
	ListCell *placementCell = NULL;
	int datumCount = list_length(placementList);
	int datumIndex = 0;
	Datum *placementDatums = palloc0(datumCount * sizeof(Datum));
	Oid datumTypeId = TEXTOID;
	StringInfo placementInfo = makeStringInfo();

	foreach(placementCell, placementList)
	{
		ShardPlacement *placement = (ShardPlacement *) lfirst(placementCell);
		appendStringInfo(placementInfo, "%s:%d", placement->nodeName,
						 placement->nodePort);

		placementDatums[datumIndex] = CStringGetTextDatum(placementInfo->data);
		datumIndex++;
		resetStringInfo(placementInfo);
	}

	placementArrayType = DatumArrayToArrayType(placementDatums, datumCount, datumTypeId);

	pfree(placementDatums);

	PG_RETURN_ARRAYTYPE_P(placementArrayType);
}


/*
 * DatumArrayToArrayType converts the provided Datum array (of the specified
 * length and type) into an ArrayType suitable for returning from a UDF.
 */
static ArrayType *
DatumArrayToArrayType(Datum *datumArray, int datumCount, Oid datumTypeId)
{
	ArrayType *arrayObject = NULL;
	int16 typeLength = 0;
	bool typeByValue = false;
	char typeAlignment = 0;

	get_typlenbyvalalign(datumTypeId, &typeLength, &typeByValue, &typeAlignment);
	arrayObject = construct_array(datumArray, datumCount, datumTypeId,
								  typeLength, typeByValue, typeAlignment);

	return arrayObject;
}
