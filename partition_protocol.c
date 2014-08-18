/*-------------------------------------------------------------------------
 *
 * partition_protocol.c
 *			pg_shard functions to create partitions
 *
 * Portions Copyright (c) 2014, Citus Data, Inc.
 *
 * IDENTIFICATION
 *			partition_protocol.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "commands/dbcommands.h"
#include "connection.h"
#include "distribution_metadata.h"
#include "ddl_commands.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "partition_protocol.h"
#include "storage/fd.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(master_create_distributed_table);


/* local function forward declarations */
static List * WorkerNodeList();
static List * SortList(List *pointerList,
					   int (*ComparisonFunction)(const void *, const void *));
static int CompareWorkerNodes(const void *leftElement, const void *rightElement);
static List * RoundRobinIndexList(uint32 beginningIndex, uint32 workerNodeCount,
								  uint32 requiredNodeCount);
static bool WorkerCreateShard(char *nodeName, uint32 nodePort, List *ddlCommandList);
static bool ExecuteRemoteCommand(PGconn *connection, const char *sqlCommand);
static text * IntegerToText(int32 value);


/*
 * master_create_distributed_table creates empty shards for the given table
 * based on the specified number initial shards. The function first gets a list
 * of candidate nodes and issues DDL commands on the nodes to create empty shard
 * placements on those nodes. The function then updates metadata on the master
 * node to make this shard (and its placements) visible. Note that the function
 * assumes the table is hash partitioned and calculates the min/max hash token
 * ranges for each shard, giving them an equal split of the hash space.
 */
Datum
master_create_distributed_table(PG_FUNCTION_ARGS)
{
	text *relationNameText = PG_GETARG_TEXT_P(0);
	text *partitionColumnText = PG_GETARG_TEXT_P(1);
	uint32 shardCount = PG_GETARG_UINT32(2);
	uint32 replicationFactor = PG_GETARG_UINT32(3);
	char *relationNameString = text_to_cstring(relationNameText);
	Oid relationId = ResolveRelationId(relationNameString);
	uint32 shardIndex = 0;
	List *workerNodeList = NIL;
	List *ddlCommandList = NIL;
	uint32 workerNodeCount = 0;
	uint32 candidateNodeCount = 0;
	uint32 hashTokenIncrement = UINT_MAX / shardCount;

	/* verify column exists in given table */
	char *partitionColumnName = text_to_cstring(partitionColumnText);
	AttrNumber partitionColumnId = get_attnum(relationId, partitionColumnName);
	if (partitionColumnId == InvalidAttrNumber)
	{
		ereport(ERROR, (errmsg("could not find column: %s", partitionColumnName)));
	}

	/* insert row into the partition table */
	InsertPartitionRow(relationId, HASH_PARTITION_TYPE, partitionColumnText);

	/* load and sort the worker node list for deterministic placement */
	workerNodeList = WorkerNodeList();
	workerNodeList = SortList(workerNodeList, CompareWorkerNodes);

	/* make sure we don't process cancel signals until all shards are created */
	HOLD_INTERRUPTS();

	/* retrieve the DDL commands for the table */
	ddlCommandList = TableDDLCommandList(relationId);

	workerNodeCount = list_length(workerNodeList);
	if (replicationFactor > workerNodeCount)
	{
		ereport(ERROR, (errmsg("could not create distributed table"),
						(errdetail("replication factor: %d is higher than worker"
								   " node count: %d", replicationFactor,
								   workerNodeCount))));
	}

	/* if we have enough nodes, add an extra candidate node as backup */
	if (workerNodeCount > replicationFactor)
	{
		candidateNodeCount = replicationFactor + 1;
	}

	for (shardIndex = 0; shardIndex < shardCount; shardIndex++)
	{
		List *nodeIndexList = NIL;
		ListCell *nodeIndexCell = NULL;
		uint32 placementCount = 0;
		uint64 shardId = NewShardId();

		List *extendedDDLCommands = ExtendedDDLCommandList(relationId, shardId,
														   ddlCommandList);

		/* initialize the hash token space for this shard */
		int32 shardMinHashToken = INT_MIN + (shardIndex * hashTokenIncrement);
		int32 shardMaxHashToken = shardMinHashToken + hashTokenIncrement - 1;
		text *minHashTokenText = IntegerToText(shardMinHashToken);
		text *maxHashTokenText = IntegerToText(shardMaxHashToken);

		/* if we are at the last shard, make sure the max token value is INT_MAX */
		if (shardMaxHashToken > INT_MAX || shardIndex == (shardCount - 1))
		{
			shardMaxHashToken = INT_MAX;
		}

		/* get a list of indexes for candidate nodes */
		nodeIndexList = RoundRobinIndexList(shardIndex, workerNodeCount,
											candidateNodeCount);

		foreach(nodeIndexCell, nodeIndexList)
		{
			int candidateNodeIndex = lfirst_int(nodeIndexCell);
			WorkerNode *candidateNode = (WorkerNode *) list_nth(workerNodeList,
																candidateNodeIndex);
			char *nodeName = candidateNode->nodeName;
			uint32 nodePort = candidateNode->nodePort;

			bool created = WorkerCreateShard(nodeName, nodePort, extendedDDLCommands);
			if (created)
			{
				uint64 shardPlacementId = NewShardPlacementId();
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
		InsertShardRow(relationId, shardId, SHARD_STORAGE_TABLE, minHashTokenText,
					   maxHashTokenText);
	}

	if (QueryCancelPending)
	{
		ereport(WARNING, (errmsg("cancel requests are ignored during shard creation")));
		QueryCancelPending = false;
	}

	RESUME_INTERRUPTS();

	PG_RETURN_VOID();
}


/*
 * WorkerNodeList opens and parses the node name and node port from the
 * pg_worker_list.conf configuration file. The function relies on the file being
 * at the top leve in the data directory.
 */
static List *
WorkerNodeList()
{
	List *workerNodeList = NIL;
	char workerNodeLine[MAXPGPATH];
	char *workerFileName = make_absolute_path(WORKER_LIST_FILENAME);

	FILE *workerFileStream = AllocateFile(workerFileName, PG_BINARY_R);
	if (workerFileStream == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_CONFIG_FILE_ERROR),
						errmsg("could not open worker file: %s", workerFileName)));
	}

	while (fgets(workerNodeLine, sizeof(workerNodeLine), workerFileStream))
	{
		WorkerNode *workerNode = NULL;
		char firstCharacter = '\0';
		uint32 nodePort = 0;
		int parsedValues = 0;
		char nodeName[MAX_NODE_LENGTH];
		memset(nodeName, 0, MAX_NODE_LENGTH);

		if (strnlen(workerNodeLine, MAXPGPATH) == MAXPGPATH - 1)
		{
			ereport(ERROR, (errcode(ERRCODE_CONFIG_FILE_ERROR),
					 errmsg("worker node list file line too long")));
		}

		/* skip all comments and empty lines */
		firstCharacter = workerNodeLine[0];
		if (firstCharacter == '#' || strlen(workerNodeLine) == 1)
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
 * RoundRobinIndexList returns a list of indexes which can be used to retrieve
 * nodes for shard placement from the worker node list. The function assigns the
 * indexes using a round-robin policy and ensures the indexes wrap around the
 * end of the list back to the beginning.
 */
static List *
RoundRobinIndexList(uint32 beginningIndex, uint32 workerNodeCount,
					uint32 requiredNodeCount)
{
	List *roundRobinIndexList = NIL;
	uint32 nodeIndexCount = 0;

	for (nodeIndexCount = 0; nodeIndexCount < requiredNodeCount; nodeIndexCount++)
	{
		int32 nodeIndex = beginningIndex + nodeIndexCount;
		int32 workerNodeIndex = nodeIndex % workerNodeCount;

		roundRobinIndexList = lappend_int(roundRobinIndexList, workerNodeIndex);
	}

	return roundRobinIndexList;
}


/*
 * WorkerCreateShard applies DDL commands for the given shardId to create the
 * shard on the worker node. Note that this function opens a new connection for
 * each DDL command, and could leave the shard in an half-initialized state.
 */
static bool
WorkerCreateShard(char *nodeName, uint32 nodePort, List *ddlCommandList)
{
	bool shardCreated = true;
	ListCell *ddlCommandCell = NULL;
	bool commandSuccessful = false;

	PGconn *connection = GetConnection(nodeName, nodePort);
	if (connection == NULL)
	{
		return false;
	}

	/* begin a transaction before we start applying the ddl commands */
	ExecuteRemoteCommand(connection, BEGIN_COMMAND);

	foreach(ddlCommandCell, ddlCommandList)
	{
		char *ddlCommand = (char *) lfirst(ddlCommandCell);

		commandSuccessful = ExecuteRemoteCommand(connection, ddlCommand);
		if (!commandSuccessful)
		{
			break;
		}
	}

	if (commandSuccessful)
	{
		shardCreated = ExecuteRemoteCommand(connection, COMMIT_COMMAND);
	}
	else
	{
		shardCreated = false;
		ExecuteRemoteCommand(connection, ROLLBACK_COMMAND);
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
	bool commandSuccessful = true;
	PGresult *result = NULL;

	result = PQexec(connection, sqlCommand);

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
