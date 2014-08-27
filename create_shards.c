/*-------------------------------------------------------------------------
 *
 * create_shards.c
 * pg_shard functions to create a distributed table and initialize shards for
 * that table.
 *
 * Copyright (c) 2014, Citus Data, Inc.
 *
 * IDENTIFICATION
 *			create_shards.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "pg_config_manual.h"
#include "port.h"
#include "postgres_ext.h"

#include "connection.h"
#include "create_shards.h"
#include "ddl_commands.h"
#include "distribution_metadata.h"

#include <ctype.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>

#include "access/attnum.h"
#include "catalog/namespace.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "storage/fd.h"
#include "storage/lock.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/lsyscache.h"
#include "utils/palloc.h"


/* local function forward declarations */
static Oid ResolveRelationId(text *relationName);
static void CheckHashPartitionedTable(Oid distributedTableId);
static List * ParseWorkerNodeFile(char *workerNodeFilename);
static List * SortList(List *pointerList,
					   int (*ComparisonFunction)(const void *, const void *));
static int CompareWorkerNodes(const void *leftElement, const void *rightElement);
static bool WorkerCreateShard(char *nodeName, uint32 nodePort, List *ddlCommandList);
static bool ExecuteRemoteCommand(PGconn *connection, const char *sqlCommand);
static text * IntegerToText(int32 value);


/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(create_distributed_table);
PG_FUNCTION_INFO_V1(create_shards);


/*
 * create_distributed_table inserts the table and partition column information
 * into the partition metadata table. Note that this function currently assumes
 * the table is hash partitioned.
 */
Datum
create_distributed_table(PG_FUNCTION_ARGS)
{
	text *tableNameText = PG_GETARG_TEXT_P(0);
	text *partitionColumnText = PG_GETARG_TEXT_P(1);
	char partitionMethod = PG_GETARG_CHAR(2);
	Oid distributedTableId = ResolveRelationId(tableNameText);

	/* verify column exists in given table */
	char *partitionColumnName = text_to_cstring(partitionColumnText);
	AttrNumber partitionColumnId = get_attnum(distributedTableId, partitionColumnName);
	if (partitionColumnId == InvalidAttrNumber)
	{
		ereport(ERROR, (errmsg("could not find column: %s", partitionColumnName)));
	}

	/* we only support hash partitioning method for now */
	if (partitionMethod != HASH_PARTITION_TYPE)
	{
		ereport(ERROR, (errmsg("unsupported partition method: %c", partitionMethod)));
	}

	/* insert row into the partition metadata table */
	InsertPartitionRow(distributedTableId, partitionMethod, partitionColumnText);

	PG_RETURN_VOID();
}


/*
 * create_shards creates empty shards for the given table based on the specified
 * number of initial shards. The function first gets a list of candidate nodes
 * and issues DDL commands on the nodes to create empty shard placements on
 * those nodes. The function then updates metadata on the master node to make
 * this shard (and its placements) visible. Note that the function assumes the
 * table is hash partitioned and calculates the min/max hash token ranges for
 * each shard, giving them an equal split of the hash space.
 */
Datum
create_shards(PG_FUNCTION_ARGS)
{
	text *tableNameText = PG_GETARG_TEXT_P(0);
	uint32 shardCount = PG_GETARG_UINT32(1);
	uint32 replicationFactor = PG_GETARG_UINT32(2);

	Oid distributedTableId = ResolveRelationId(tableNameText);
	uint32 shardIndex = 0;
	List *workerNodeList = NIL;
	List *ddlCommandList = NIL;
	uint32 workerNodeCount = 0;
 	uint32 placementAttemptCount = 0;
	uint32 hashTokenIncrement = UINT_MAX / shardCount;
	List *existingShardList = NIL;

	/* make sure table is hash partitioned */
	CheckHashPartitionedTable(distributedTableId);

	/* validate that shards haven't already been created for this table */
	existingShardList = LoadShardList(distributedTableId);
	if (existingShardList != NIL)
	{
		ereport(ERROR, (errmsg("cannot create new shards for table"),
						(errdetail("Shards have already been created"))));
	}

	/* load and sort the worker node list for deterministic placement */
	workerNodeList = ParseWorkerNodeFile(WORKER_LIST_FILENAME);
	workerNodeList = SortList(workerNodeList, CompareWorkerNodes);

	/* make sure we don't process cancel signals until all shards are created */
	HOLD_INTERRUPTS();

	/* retrieve the DDL commands for the table */
	ddlCommandList = TableDDLCommandList(distributedTableId);

	workerNodeCount = list_length(workerNodeList);
	if (replicationFactor > workerNodeCount)
	{
		ereport(ERROR, (errmsg("could not create distributed table"),
						(errdetail("Replication factor: %u exceeds worker node count: %u",
								   replicationFactor, workerNodeCount))));
	}

	/* if we have enough nodes, add an extra placement attempt for backup */
	if (workerNodeCount > replicationFactor)
	{
		placementAttemptCount = replicationFactor + 1;
	}

	for (shardIndex = 0; shardIndex < shardCount; shardIndex++)
	{
		uint64 shardId = NextSequenceId(SHARD_ID_SEQUENCE_NAME);
		uint32 placementCount = 0;
		uint32 placementIndex = 0;
		uint32 roundRobinNodeIndex = shardIndex % workerNodeCount;

		List *extendedDDLCommands = ExtendedDDLCommandList(distributedTableId, shardId,
														   ddlCommandList);

		/* initialize the hash token space for this shard */
		text *minHashTokenText = NULL;
		text *maxHashTokenText = NULL;
		int32 shardMinHashToken = INT_MIN + (shardIndex * hashTokenIncrement);
		int32 shardMaxHashToken = shardMinHashToken + hashTokenIncrement - 1;

		/* if we are at the last shard, make sure the max token value is INT_MAX */
		if (shardIndex == (shardCount - 1))
		{
			shardMaxHashToken = INT_MAX;
		}

		for (placementIndex = 0; placementIndex < placementAttemptCount; placementIndex++)
		{
			int32 candidateNodeIndex =
				(roundRobinNodeIndex + placementIndex) % workerNodeCount;
			WorkerNode *candidateNode = (WorkerNode *) list_nth(workerNodeList,
																candidateNodeIndex);
			char *nodeName = candidateNode->nodeName;
			uint32 nodePort = candidateNode->nodePort;

			bool created = WorkerCreateShard(nodeName, nodePort, extendedDDLCommands);
			if (created)
			{
				uint64 shardPlacementId = NextSequenceId(SHARD_PLACEMENT_ID_SEQUENCE_NAME);
				ShardState shardState = STATE_FINALIZED;

				InsertShardPlacementRow(shardPlacementId, shardId, shardState,
										nodeName, nodePort);
				placementCount++;
			}
			else
			{
				ereport(WARNING, (errmsg("could not create shard on \"%s:%u\"",
										 nodeName, nodePort)));
			}

			if (placementCount >= replicationFactor)
			{
				break;
			}
		}

		/* check if we created enough shard replicas */
		if (placementCount < replicationFactor)
		{
			ereport(ERROR, (errmsg("could only create %u of %u of required shard replicas",
								   placementCount, replicationFactor)));
		}

		/* insert the shard metadata row along with its min/max values */
		minHashTokenText = IntegerToText(shardMinHashToken);
		maxHashTokenText = IntegerToText(shardMaxHashToken);
		InsertShardRow(distributedTableId, shardId, SHARD_STORAGE_TABLE,
					   minHashTokenText, maxHashTokenText);
	}

	if (QueryCancelPending)
	{
		ereport(WARNING, (errmsg("cancel requests are ignored during shard creation")));
		QueryCancelPending = false;
	}

	RESUME_INTERRUPTS();

	PG_RETURN_VOID();
}


/* Finds the relationId from a potentially qualified relation name. */
static Oid
ResolveRelationId(text *relationName)
{
	List *relationNameList = NIL;
	RangeVar *relation = NULL;
	Oid  relationId = InvalidOid;
	bool failOK = false;		/* error if relation cannot be found */

	/* resolve relationId from passed in schema and relation name */
	relationNameList = textToQualifiedNameList(relationName);
	relation = makeRangeVarFromNameList(relationNameList);
	relationId = RangeVarGetRelid(relation, NoLock, failOK);

	return relationId;
}


/*
 * CheckHashPartitionedTable looks up the partition information for the given
 * tableId and checks if the table is hash partitioned. If not, the function
 * throws an error.
 */
static void
CheckHashPartitionedTable(Oid distributedTableId)
{
	char partitionType = PartitionType(distributedTableId);
	if (partitionType != HASH_PARTITION_TYPE)
	{
		ereport(ERROR, (errmsg("unsupported table partition type: %c", partitionType)));
	}
}


/*
 * ParseWorkerNodeFile opens and parses the node name and node port from the
 * specified configuration file. The function relies on the file being at the
 * top level in the data directory.
 */
static List *
ParseWorkerNodeFile(char *workerNodeFilename)
{
	List *workerNodeList = NIL;
	char workerNodeLine[MAXPGPATH];
	char *workerFilePath = make_absolute_path(workerNodeFilename);

	FILE *workerFileStream = AllocateFile(workerFilePath, PG_BINARY_R);
	if (workerFileStream == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_CONFIG_FILE_ERROR),
						errmsg("could not open worker file: %s", workerFilePath)));
	}

	while (fgets(workerNodeLine, sizeof(workerNodeLine), workerFileStream) != NULL)
	{
		WorkerNode *workerNode = NULL;
		char *linePointer = NULL;
		uint32 nodePort = 0;
		int parsedValues = 0;
		char nodeName[MAX_NODE_LENGTH];
		memset(nodeName, 0, MAX_NODE_LENGTH);

		if (strnlen(workerNodeLine, MAXPGPATH) == MAXPGPATH - 1)
		{
			ereport(ERROR, (errcode(ERRCODE_CONFIG_FILE_ERROR),
							errmsg("worker node list file line too long")));
		}

		/* skip leading whitespace and check for # comment */
		for (linePointer = workerNodeLine; *linePointer; linePointer++)
		{
			if (!isspace((unsigned char) *linePointer))
			{
				break;
			}
		}

		if (*linePointer == '\0' || *linePointer == '#')
		{
			continue;
		}

		/* parse out the node name and node port */
		parsedValues = sscanf(workerNodeLine, "%s%u", nodeName, &nodePort);
		if (parsedValues != 2)
		{
			ereport(ERROR, (errmsg("unable to parse worker node line: %s",
								   workerNodeLine)));
		}

		/* allocate worker node structure and set fields */
		workerNode = (WorkerNode *) palloc0(sizeof(WorkerNode));
		workerNode->nodeName = palloc(sizeof(char) * MAX_NODE_LENGTH);
		strlcpy(workerNode->nodeName, nodeName, MAX_NODE_LENGTH);
		workerNode->nodePort = nodePort;

		workerNodeList = lappend(workerNodeList, workerNode);
	}

	FreeFile(workerFileStream);
	return workerNodeList;
}


/*
 * SortList takes in a list of void pointers, and sorts these pointers (and the
 * values they point to) by applying the given comparison function. The function
 * then returns the sorted list of pointers.
 */
static List *
SortList(List *pointerList, int (*ComparisonFunction)(const void *, const void *))
{
	List *sortedList = NIL;
	uint32 arrayIndex = 0;
	uint32 arraySize = (uint32) list_length(pointerList);
	void **array = (void **) palloc0(arraySize * sizeof(void *));

	ListCell *pointerCell = NULL;
	foreach(pointerCell, pointerList)
	{
		void *pointer = lfirst(pointerCell);
		array[arrayIndex] = pointer;

		arrayIndex++;
	}

	/* sort the array of pointers using the comparison function */
	qsort(array, arraySize, sizeof(void *), ComparisonFunction);

	/* convert the sorted array of pointers back to a sorted list */
	for (arrayIndex = 0; arrayIndex < arraySize; arrayIndex++)
	{
		void *sortedPointer = array[arrayIndex];
		sortedList = lappend(sortedList, sortedPointer);
	}

	return sortedList;
}


/* Helper function to compare two workers by their node name and port number. */
static int
CompareWorkerNodes(const void *leftElement, const void *rightElement)
{
	const WorkerNode *leftNode = *((const WorkerNode **) leftElement);
	const WorkerNode *rightNode = *((const WorkerNode **) rightElement);

	int nameCompare = 0;
	int portCompare = 0;

	nameCompare = strncmp(leftNode->nodeName, rightNode->nodeName, MAX_NODE_LENGTH);
	if (nameCompare != 0)
	{
		return nameCompare;
	}

	portCompare = (int) (leftNode->nodePort - rightNode->nodePort);
	return portCompare;
}


/*
 * WorkerCreateShard applies the given DDL commands to create the shard on the
 * worker node.
 */
static bool
WorkerCreateShard(char *nodeName, uint32 nodePort, List *ddlCommandList)
{
	bool shardCreated = true;
	ListCell *ddlCommandCell = NULL;
	bool ddlCommandIssued = false;
	bool beginIssued = false;

	PGconn *connection = GetConnection(nodeName, nodePort);
	if (connection == NULL)
	{
		return false;
	}

	/* begin a transaction before we start applying the ddl commands */
	beginIssued = ExecuteRemoteCommand(connection, BEGIN_COMMAND);
	if (!beginIssued)
	{
		return false;
	}

	foreach(ddlCommandCell, ddlCommandList)
	{
		char *ddlCommand = (char *) lfirst(ddlCommandCell);

		ddlCommandIssued = ExecuteRemoteCommand(connection, ddlCommand);
		if (!ddlCommandIssued)
		{
			break;
		}
	}

	if (ddlCommandIssued)
	{
		bool commitIssued = ExecuteRemoteCommand(connection, COMMIT_COMMAND);
		if (!commitIssued)
		{
			shardCreated = false;
		}
	}
	else
	{
		ExecuteRemoteCommand(connection, ROLLBACK_COMMAND);
		shardCreated = false;
	}

	return shardCreated;
}


/*
 * ExecuteRemoteCommand executes the given sql command on the remote node, and
 * returns true if the command executed successfully. Note that the function
 * assumes the command does not return tuples.
 */
static bool
ExecuteRemoteCommand(PGconn *connection, const char *sqlCommand)
{
	PGresult *result = PQexec(connection, sqlCommand);
	bool commandSuccessful = true;

	if (PQresultStatus(result) != PGRES_COMMAND_OK)
	{
		ReportRemoteError(connection, result);
		commandSuccessful = false;
	}

	PQclear(result);
	return commandSuccessful;
}


/* Helper function to convert an integer value to a text type */
static text *
IntegerToText(int32 value)
{
	text *valueText = NULL;
	StringInfo valueString = makeStringInfo();
	appendStringInfo(valueString, "%d", value);

	valueText = cstring_to_text(valueString->data);

	return valueText;
}
