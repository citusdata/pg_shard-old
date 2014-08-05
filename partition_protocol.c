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

#include "access/heapam.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "commands/dbcommands.h"
#include "commands/sequence.h"
#include "connection.h"
#include "distribution_metadata.h"
#include "extend_ddl_commands.h"
#include "generate_ddl_commands.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "partition_protocol.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(master_create_distributed_table);


/* local function forward declarations */
static Oid ResolveRelationId(text *relationName);
static List * WorkerNodeList();
static List * SortList(List *pointerList, int (*ComparisonFunction)(const void *, const void *));
static int CompareWorkerNodes(const void *leftElement, const void *rightElement);
static void InsertPartitionRow(Oid relationId, text *partitionKeyText);
static uint64 ShardId();
static List * GetCandidateNodes(List *workerNodeList, uint32 beginningOffset,
								uint32 requiredNodeCount);
static bool WorkerCreateShard(char *nodeName, uint32 nodePort, List *ddlCommandList);
static bool ExecuteRemoteCommand(const char *nodeName, uint32 nodePort,
								 StringInfo sqlCommand);
static uint64 ShardPlacementId();
static void InsertShardPlacementRow(uint64 shardPlacementId, uint64 shardId,
									ShardState shardState, char *nodeName, uint32 nodePort);
static text * ConvertIntegerToText(uint32 value);
static void InsertShardRow(Oid relationId, uint64 shardId, char shardStorage,
						   text *shardMinValue, text *shardMaxValue);


/*
 * master_create_distributed_table creates empty shards for the given table
 * based on the specified number initial shards. The function first gets a list
 * of candidate nodes and issues DDL commands on the nodes to create empty shard
 * placements on those nodes. The function then updates metadata on the master
 * node to make this shard (and its placements) visible. Note that the function
 * assumes the table is hash partitioned and calculates the min/max values for
 * each shard, giving them an equal split of the hash space.
 */
Datum
master_create_distributed_table(PG_FUNCTION_ARGS)
{
    text *relationName = PG_GETARG_TEXT_P(0);
    text *partitionColumnName = PG_GETARG_TEXT_P(1);
    uint32 shardCount = PG_GETARG_UINT32(2);
    uint32 replicationFactor = PG_GETARG_UINT32(3);
 	Oid relationId = ResolveRelationId(relationName);
	uint32 workerNodeIndex = 0;
	List *workerNodeList = NIL;
	List *ddlCommandList = NIL;

	/* split the hash space by the number of shards and set the first min/max values */
	uint32 hashValueIncrement = UINT_MAX / shardCount;
	uint32 shardMinValue = 0;
	uint32 shardMaxValue = hashValueIncrement;
    uint32 shardCreatedCount = 0;

	/* verify column exists in given table */
	char *partitionColumnString = text_to_cstring(partitionColumnName);
	AttrNumber attributeNumber = get_attnum(relationId, partitionColumnString);
	if (attributeNumber == InvalidAttrNumber)
	{
		ereport(ERROR, (errmsg("could not find column: %s", partitionColumnString)));
	}

	/* insert row into the partition table */
	InsertPartitionRow(relationId, partitionColumnName);

	/* load and sort the worker node list for deterministic placement */
	workerNodeList = WorkerNodeList();
	workerNodeList = SortList(workerNodeList, CompareWorkerNodes);

	/* make sure we don't process cancel signals until all shards are created */
	HOLD_INTERRUPTS();

	/* retrieve the DDL commands for the table */
	ddlCommandList = TableDDLCommandList(relationId);

	while (shardCreatedCount < shardCount)
	{
		text *minValueText = NULL;
		text *maxValueText = NULL;
		uint32 placementCount = 0;

		/* obtain a new shard id */
		uint64 shardId = ShardId();

		/* extend the names in the DDL events with the shardId */
		List *extendedDDLCommands = ExtendedDDLCommandList(relationId, shardId,
														   ddlCommandList);

		ListCell *candidateNodeCell = NULL;
		List *candidateNodeList = GetCandidateNodes(workerNodeList,	workerNodeIndex,
													replicationFactor);

		foreach(candidateNodeCell, candidateNodeList)
		{
			WorkerNode *candidateNode = (WorkerNode *) lfirst(candidateNodeCell);
			char *nodeName = candidateNode->nodeName;
			uint32 nodePort = candidateNode->nodePort;

			bool created = WorkerCreateShard(nodeName, nodePort, extendedDDLCommands);
			if (created)
			{
				uint64 shardPlacementId = ShardPlacementId();
				ShardState shardState = STATE_FINALIZED;
				InsertShardPlacementRow(shardPlacementId, shardId, shardState, nodeName, nodePort);
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

		/* create the shard metadata along with its min/max values */
		minValueText = ConvertIntegerToText(shardMinValue);
		maxValueText = ConvertIntegerToText(shardMaxValue);
		InsertShardRow(relationId, shardId, SHARD_STORAGE_TABLE, minValueText, maxValueText);

		shardCreatedCount++;
		workerNodeIndex++;

		/* update the min and max values for the next shard*/
		shardMinValue = shardMaxValue + 1;
		shardMaxValue = shardMaxValue + hashValueIncrement;

		/* if we are at the last shard, make sure the max value is UINT_MAX */
		if (shardMaxValue > UINT_MAX || shardCreatedCount == (shardCount - 1))
		{
			shardMaxValue = UINT_MAX;
		}
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
 * GetCandidateNodes returns a list of candidate nodes from the worker node
 * list for placing the new shard. The function starts from the beginning offset
 * and returns the number of nodes as specified by the required node count.
 */
static List *
GetCandidateNodes(List *workerNodeList, uint32 beginningOffset, uint32 requiredNodeCount)
{
	List *candidateNodeList = NIL;
	uint32 workerNodeOffset = beginningOffset;
	uint32 workerNodeCount = (uint32) list_length(workerNodeList);
	uint32 candidateNodeCount = 0;
	uint32 attemptableNodeCount = requiredNodeCount;

	if (requiredNodeCount > workerNodeCount)
	{
		ereport(ERROR, (errmsg("could not retrieve candidate nodes for shard"),
						(errdetail("required node count: %d is higher than worker"
								   " node count: %d", requiredNodeCount,
								   workerNodeCount))));
	}

	/* if we have enough nodes, add an extra candidate node as backup */
	if (workerNodeCount > requiredNodeCount)
	{
		attemptableNodeCount = requiredNodeCount + 1;
	}

	/*
	 * Loop over the list beginning at the initial offset and add the required
	 * nodes to the list. If the end of the list is reached we loop back to the
	 * beginning.
	*/
	while (candidateNodeCount < attemptableNodeCount)
	{
		uint32 workerNodeIndex = workerNodeOffset % workerNodeCount;
		WorkerNode *candidateNode = (WorkerNode *) list_nth(workerNodeList,
															(int) workerNodeIndex);
		candidateNodeList = lappend(candidateNodeList, candidateNode);
		candidateNodeCount++;

		workerNodeOffset++;
	}

	return candidateNodeList;
}


/* Helper function to convert an integer value to a text type */
static text *
ConvertIntegerToText(uint32 value)
{
	text *integerText = NULL;
	StringInfo integerString = makeStringInfo();
	appendStringInfo(integerString, "%u", value);

	integerText = cstring_to_text(integerString->data);

	return integerText;
}


/*
 * InsertPartitionRow opens the partition metadata table and inserts a new row
 * with the given values.
 */
static void
InsertPartitionRow(Oid relationId, text *partitionKeyText)
{
	Relation pgPartition = NULL;
	TupleDesc tupleDescriptor = NULL;
	HeapTuple heapTuple = NULL;
	Datum values[PARTITION_ATTRIBUTE_COUNT];
	bool isNulls[PARTITION_ATTRIBUTE_COUNT];
	StringInfo partitionTableString = makeStringInfo();
	appendStringInfo(partitionTableString, "%s.%s", METADATA_SCHEMA_NAME,
					 PARTITION_TABLE_NAME);
	text *partitionTableName = cstring_to_text(partitionTableString->data);
	Oid partitionRelationId = ResolveRelationId(partitionTableName);

	/* form new shard tuple */
	memset(values, 0, sizeof(values));
	memset(isNulls, false, sizeof(isNulls));

	values[ATTR_NUM_PARTITION_RELATION_ID - 1] = ObjectIdGetDatum(relationId);
	values[ATTR_NUM_PARTITION_KEY - 1] = PointerGetDatum(partitionKeyText);

	/* open shard relation and insert new tuple */
	pgPartition = heap_open(partitionRelationId, RowExclusiveLock);

	tupleDescriptor = RelationGetDescr(pgPartition);
	heapTuple = heap_form_tuple(tupleDescriptor, values, isNulls);

	simple_heap_insert(pgPartition, heapTuple);
	CatalogUpdateIndexes(pgPartition, heapTuple);
	CommandCounterIncrement();

	/* close relation */
	heap_close(pgPartition, RowExclusiveLock);
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

	foreach(ddlCommandCell, ddlCommandList)
	{
		char *ddlCommand = (char *) lfirst(ddlCommandCell);
		bool commandSuccessful = false;

		StringInfo applyDDLCommand = makeStringInfo();
		appendStringInfoString(applyDDLCommand, ddlCommand);

		commandSuccessful = ExecuteRemoteCommand(nodeName, nodePort, applyDDLCommand);
		if (!commandSuccessful)
		{
			shardCreated = false;
			break;
		}
	}

	return shardCreated;
}


/*
 * InsertShardRow opens the shard metadata table and inserts a new row with
 * the given values into that table. Note that we allow the user to pass in
 * null min/max values.
 */
static void
InsertShardRow(Oid relationId, uint64 shardId, char shardStorage,
			   text *shardMinValue, text *shardMaxValue)
{
	Relation pgShard = NULL;
	TupleDesc tupleDescriptor = NULL;
	HeapTuple heapTuple = NULL;
	Datum values[SHARD_ATTRIBUTE_COUNT];
	bool isNulls[SHARD_ATTRIBUTE_COUNT];

	StringInfo shardStorageString = makeStringInfo();
	text *shardStorageText = NULL;
	StringInfo shardTableString = makeStringInfo();
	text *shardTableName = NULL;
	Oid shardRelationId = InvalidOid;

	appendStringInfo(shardTableString, "%s.%s", METADATA_SCHEMA_NAME,
					 SHARD_TABLE_NAME);
	shardTableName = cstring_to_text(shardTableString->data);
	shardRelationId = ResolveRelationId(shardTableName);

	/* convert the shard storage char into a text type */
	appendStringInfoChar(shardStorageString, shardStorage);
	shardStorageText = cstring_to_text(shardStorageString->data);

	/* form new shard tuple */
	memset(values, 0, sizeof(values));
	memset(isNulls, false, sizeof(isNulls));

	values[ATTR_NUM_SHARD_ID - 1] = Int64GetDatum(shardId);
	values[ATTR_NUM_SHARD_RELATION_ID - 1] = ObjectIdGetDatum(relationId);
	values[ATTR_NUM_SHARD_STORAGE - 1] = PointerGetDatum(shardStorageText);

	/* check if shard min/max values are null */
	if (shardMinValue != NULL && shardMaxValue != NULL)
	{
		values[ATTR_NUM_SHARD_MIN_VALUE - 1] = PointerGetDatum(shardMinValue);
		values[ATTR_NUM_SHARD_MAX_VALUE - 1] = PointerGetDatum(shardMaxValue);
	}
	else
	{
		isNulls[ATTR_NUM_SHARD_MIN_VALUE - 1] = true;
		isNulls[ATTR_NUM_SHARD_MAX_VALUE - 1] = true;
	}

	/* open shard relation and insert new tuple */
	pgShard = heap_open(shardRelationId, RowExclusiveLock);

	tupleDescriptor = RelationGetDescr(pgShard);
	heapTuple = heap_form_tuple(tupleDescriptor, values, isNulls);

	simple_heap_insert(pgShard, heapTuple);
	CatalogUpdateIndexes(pgShard, heapTuple);
	CommandCounterIncrement();
	
	/* close relation */
	heap_close(pgShard, RowExclusiveLock);
}


/*
 * InsertShardPlacementRow opens the shard placement metadata table and inserts
 * a row with the given values into the table.
 */
static void
InsertShardPlacementRow(uint64 shardPlacementId, uint64 shardId,
						ShardState shardState, char *nodeName, uint32 nodePort)
{
	Relation pgShardPlacement = NULL;
	TupleDesc tupleDescriptor = NULL;
	HeapTuple heapTuple = NULL;
	Datum values[SHARD_PLACEMENT_ATTRIBUTE_COUNT];
	bool isNulls[SHARD_PLACEMENT_ATTRIBUTE_COUNT];

	StringInfo placementTableString = makeStringInfo();
	text *shardPlacementTableName = NULL;
	Oid shardPlacementRelationId = InvalidOid;

	appendStringInfo(placementTableString, "%s.%s", METADATA_SCHEMA_NAME,
					 SHARD_PLACEMENT_TABLE_NAME);
	shardPlacementTableName = cstring_to_text(placementTableString->data);
	shardPlacementRelationId = ResolveRelationId(shardPlacementTableName);

	/* form new shard placement tuple */
	memset(values, 0, sizeof(values));
	memset(isNulls, false, sizeof(isNulls));

	values[ATTR_NUM_SHARD_PLACEMENT_ID - 1] = Int64GetDatum(shardPlacementId);
	values[ATTR_NUM_SHARD_PLACEMENT_SHARD_ID - 1] = Int64GetDatum(shardId);
	values[ATTR_NUM_SHARD_PLACEMENT_SHARD_STATE - 1] = UInt32GetDatum(shardState);
	values[ATTR_NUM_SHARD_PLACEMENT_NODE_NAME - 1] = CStringGetTextDatum(nodeName);
	values[ATTR_NUM_SHARD_PLACEMENT_NODE_PORT - 1] = UInt32GetDatum(nodePort);

	/* open shard placement relation and insert new tuple */
	pgShardPlacement = heap_open(shardPlacementRelationId, RowExclusiveLock);

	tupleDescriptor = RelationGetDescr(pgShardPlacement);
	heapTuple = heap_form_tuple(tupleDescriptor, values, isNulls);

	simple_heap_insert(pgShardPlacement, heapTuple);
	CatalogUpdateIndexes(pgShardPlacement, heapTuple);
	CommandCounterIncrement();

	/* close relation */
	heap_close(pgShardPlacement, RowExclusiveLock);
}


/* Helper function to compare two workers by their node name and port number. */
static int
CompareWorkerNodes(const void *leftElement, const void *rightElement)
{
	const WorkerNode *leftNode = *((const WorkerNode **) leftElement);
	const WorkerNode *rightNode = *((const WorkerNode **) rightElement);

	int nameCompare = 0;
	int portCompare = 0;

	nameCompare = strncmp(leftNode->nodeName, rightNode->nodeName, NODE_NAME_LENGTH);
	if (nameCompare != 0)
	{
		return nameCompare;
	}

	portCompare = (int) (leftNode->nodePort - rightNode->nodePort);
	return portCompare;
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

	char *workerFileName = make_absolute_path(WORKER_LIST_FILENAME);
	FILE *workerFileStream = fopen(workerFileName, PG_BINARY_R);
	if (workerFileStream == NULL)
	{
		ereport(FATAL, (errcode(ERRCODE_CONFIG_FILE_ERROR),
						errmsg("could not open worker file: %s", workerFileName)));
	}

	while (!feof(workerFileStream) && !ferror(workerFileStream))
	{
		char workerNodeLine[MAX_WORKER_NODE_LINE];
		WorkerNode *workerNode = NULL;
		char *nodeName = NULL;
		char *nodePortString = NULL;
		char *nodePortEnd = NULL;
		uint32 nodePort = 0;
		char firstCharacter = '\0';

		if (!fgets(workerNodeLine, sizeof(workerNodeLine), workerFileStream))
		{
			break;
		}

		if (strlen(workerNodeLine) == MAX_WORKER_NODE_LINE - 1)
		{
			ereport(ERROR, (errcode(ERRCODE_CONFIG_FILE_ERROR),
					 errmsg("worker node list file line too long")));
		}

		/* skip all comments */
		firstCharacter = workerNodeLine[0];
		if (firstCharacter == '#')
		{
			continue;
		}

		/* parse out the node name and node port */
		nodeName = strtok(workerNodeLine, WHITESPACE);
		nodePortString = strtok(NULL, WHITESPACE);

		if (nodeName == NULL || nodePortString == NULL)
		{
			/* XXX: skip unparsed/empty lines: should we error/warn? */
			continue;
		}

		errno = 0;
		nodePort = (uint32) strtoul(nodePortString, &nodePortEnd, 0);
		if (errno != 0 || (*nodePortEnd) != '\0')
		{
			ereport(LOG, (errmsg("line has invalid port: %s", nodePortString)));
			return NULL;
		}

		/* allocate worker node structure and set fields */
		workerNode = (WorkerNode *) palloc0(sizeof(WorkerNode));
		strlcpy(workerNode->nodeName, nodeName, NODE_NAME_LENGTH);
		workerNode->nodePort = nodePort;

		workerNodeList = lappend(workerNodeList, workerNode);
	}

	fclose(workerFileStream);
	return workerNodeList;
}


/*
 * ShardId allocates and returns a unique shardId for the shard to be
 * created. The function relies on an internal sequence created when the
 * extension is loaded to generate unique identifiers.
 */
static uint64
ShardId()
{
	StringInfo sequenceString = makeStringInfo();
	text *sequenceName = NULL;
	Oid   sequenceId = InvalidOid;
	Datum sequenceIdDatum = 0;
	Datum shardIdDatum = 0;
	uint64 shardId = 0;

	appendStringInfo(sequenceString, "%s.%s", METADATA_SCHEMA_NAME,
					 SHARD_ID_SEQUENCE_NAME);
	sequenceName = cstring_to_text(sequenceString->data);
	sequenceId = ResolveRelationId(sequenceName);
	sequenceIdDatum = ObjectIdGetDatum(sequenceId);

	/* generate new and unique shardId from sequence */
	shardIdDatum = DirectFunctionCall1(nextval_oid, sequenceIdDatum);
	shardId = (uint64) DatumGetInt64(shardIdDatum);

	return shardId;
}


/*
 * ShardPlacementId allocates and returns a unique shardPlacementId for the
 * shard placement to be created. The function relies on an internal sequence
 * created when the extension is loaded to generate unique identifiers.
 */
static uint64
ShardPlacementId()
{
	StringInfo sequenceString = makeStringInfo();
	text *sequenceName = NULL;
	Oid   sequenceId = InvalidOid;
	Datum sequenceIdDatum = 0;
	Datum shardPlacementIdDatum = 0;
	uint64 shardPlacementId = 0;

	appendStringInfo(sequenceString, "%s.%s", METADATA_SCHEMA_NAME,
					 SHARD_PLACEMENT_ID_SEQUENCE_NAME);
	sequenceName = cstring_to_text(sequenceString->data);
	sequenceId = ResolveRelationId(sequenceName);
	sequenceIdDatum = ObjectIdGetDatum(sequenceId);

	/* generate new and unique shardId from sequence */
	shardPlacementIdDatum = DirectFunctionCall1(nextval_oid, sequenceIdDatum);
	shardPlacementId = (uint64) DatumGetInt64(shardPlacementIdDatum);

	return shardPlacementId;
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
 * ExecuteRemoteCommand executes the given sql command on the remote node, and
 * returns true if the command executed successfully. Note that the function
 * assumes the command does not return tuples.
 */
static bool
ExecuteRemoteCommand(const char *nodeName, uint32 nodePort, StringInfo sqlCommand)
{
	bool commandSuccessful = true;

	PGconn *connection = GetConnection(nodeName, nodePort);

	PGresult *result = PQexec(connection, sqlCommand->data);

	if (PQresultStatus(result) != PGRES_COMMAND_OK)
	{
		commandSuccessful = false;
	}

	return commandSuccessful;
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
