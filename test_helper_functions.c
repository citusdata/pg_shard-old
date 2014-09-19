/*-------------------------------------------------------------------------
 *
 * test_helper_functions.c
 *		  Test wrapper functions for pg_shard
 *
 * Portions Copyright (c) 2014, Citus Data, Inc.
 *
 * IDENTIFICATION
 *		  test_helper_functions.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"
#include "fmgr.h"
#include "libpq-fe.h"
#include "postgres_ext.h"

#include "connection.h"
#include "distribution_metadata.h"
#include "test_helper_functions.h"

#include <stddef.h>
#include <string.h>

#include "catalog/pg_type.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/lsyscache.h"
#include "utils/palloc.h"


/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(PopulateTempTable);
PG_FUNCTION_INFO_V1(CountTempTable);
PG_FUNCTION_INFO_V1(GetAndPurgeConnection);
PG_FUNCTION_INFO_V1(LoadShardIdArray);
PG_FUNCTION_INFO_V1(LoadShardIntervalArray);
PG_FUNCTION_INFO_V1(LoadShardPlacementArray);
PG_FUNCTION_INFO_V1(PartitionColumnAttributeNumber);


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
 * and returns -1.
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
		count = Int32GetDatum(-1);
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
	Oid typIoFunc = InvalidOid;
	Oid typIoParam = InvalidOid;
	Datum intDatum = 0;
	FmgrInfo fmgrInfo;
	memset(&fmgrInfo, 0, sizeof(fmgrInfo));

	getTypeInputInfo(INT4OID, &typIoFunc, &typIoParam);
	fmgr_info(typIoFunc, &fmgrInfo);

	intDatum = InputFunctionCall(&fmgrInfo, input, typIoFunc, -1);

	return intDatum;
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
	List *shardList = LoadShardIntervalList(distributedTableId);
	ListCell *shardCell = NULL;
	int shardIdCount = list_length(shardList);
	int shardIdIndex = 0;
	Datum *shardIdDatumArray = palloc0(shardIdCount * sizeof(Datum));
	Oid shardIdTypeId = INT8OID;

	foreach(shardCell, shardList)
	{
		ShardInterval *shardId = (ShardInterval *) lfirst(shardCell);
		Datum shardIdDatum = Int64GetDatum(shardId->id);

		shardIdDatumArray[shardIdIndex] = shardIdDatum;
		shardIdIndex++;
	}

	shardIdArrayType = DatumArrayToArrayType(shardIdDatumArray, shardIdCount,
											 shardIdTypeId);

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
	int placementCount = list_length(placementList);
	int placementIndex = 0;
	Datum *placementDatumArray = palloc0(placementCount * sizeof(Datum));
	Oid placementTypeId = TEXTOID;
	StringInfo placementInfo = makeStringInfo();

	foreach(placementCell, placementList)
	{
		ShardPlacement *placement = (ShardPlacement *) lfirst(placementCell);
		appendStringInfo(placementInfo, "%s:%d", placement->nodeName,
						 placement->nodePort);

		placementDatumArray[placementIndex] = CStringGetTextDatum(placementInfo->data);
		placementIndex++;
		resetStringInfo(placementInfo);
	}

	placementArrayType = DatumArrayToArrayType(placementDatumArray, placementCount,
											   placementTypeId);

	PG_RETURN_ARRAYTYPE_P(placementArrayType);
}


/*
 * PartitionColumnAttributeNumber simply finds a distributed table using the
 * provided Oid and returns the attribute number of its partition column. If the
 * specified table is not distributed, this function raises an error instead.
 */
Datum
PartitionColumnAttributeNumber(PG_FUNCTION_ARGS)
{
	Oid distributedTableId = PG_GETARG_OID(0);
	Var *partitionColumn = PartitionColumn(distributedTableId);

	PG_RETURN_INT16((int16) partitionColumn->varattno);
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
