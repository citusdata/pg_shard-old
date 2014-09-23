/*-------------------------------------------------------------------------
 *
 * pg_shard.c
 *			Type and function declarations for pg_shard extension
 *
 * Portions Copyright (c) 2014, Citus Data, Inc.
 *
 * IDENTIFICATION
 *			pg_shard.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"
#include "fmgr.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "postgres_ext.h"

#include "pg_shard.h"
#include "connection.h"
#include "distribution_metadata.h"
#include "prune_shard_list.h"
#include "ruleutils.h"

#include <stddef.h>

#include "access/skey.h"
#include "executor/execdesc.h"
#include "executor/executor.h"
#include "nodes/nodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "nodes/primnodes.h"
#include "optimizer/clauses.h"
#include "optimizer/planner.h"
#include "parser/parsetree.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/guc.h"
#include "utils/palloc.h"


/* informs pg_shard to use the CitusDB planner */
bool UseCitusDBSelectLogic = false;


/* local function forward declarations */
static PlannedStmt * PgShardPlannerHook(Query *parse, int cursorOptions,
										ParamListInfo boundParams);
static PlannerType DeterminePlannerType(Query *query);
static Oid ExtractFirstDistributedTableId(Query *query);
static bool ExtractRangeTableEntryWalker(Node *node, List **rangeTableList);
static void ErrorIfQueryNotSupported(Query *queryTree);
static DistributedPlan * PlanDistributedQuery(Query *query);
static List * QueryRestrictList(Query *query);
static Const * ExtractPartitionValue(Query *query, Var *partitionColumn);
static bool ExtractFromExpressionWalker(Node *node, List **qualifierList);
static DistributedPlan * BuildDistributedPlan(Query *query, List *shardIntervalList);
static void PgShardExecutorStart(QueryDesc *queryDesc, int eflags);
static void PgShardExecutorRun(QueryDesc *queryDesc, ScanDirection direction,
								   long count);
static void ExecuteDistributedSelect(DistributedPlan *distributedPlan,
									 EState *executorState, DestReceiver *destination);
static Tuplestorestate * ExecuteRemoteQuery(char *nodeName, int32 nodePort,
											StringInfo query, TupleDesc tupleDescriptor);
static void PgShardExecutorFinish(QueryDesc *queryDesc);
static void PgShardExecutorEnd(QueryDesc *queryDesc);


/* declarations for dynamic loading */
PG_MODULE_MAGIC;


/* saved hook values in case of unload */
static planner_hook_type PreviousPlannerHook = NULL;
static ExecutorStart_hook_type PreviousExecutorStartHook = NULL;
static ExecutorRun_hook_type PreviousExecutorRunHook = NULL;
static ExecutorFinish_hook_type PreviousExecutorFinishHook = NULL;
static ExecutorEnd_hook_type PreviousExecutorEndHook = NULL;


/*
 * _PG_init is called when the module is loaded. In this function we save the
 * previous utility hook, and then install our hook to pre-intercept calls to
 * the copy command.
 */
void
_PG_init(void)
{
	PreviousPlannerHook = planner_hook;
	planner_hook = PgShardPlannerHook;

	PreviousExecutorStartHook = ExecutorStart_hook;
	ExecutorStart_hook = PgShardExecutorStart;

	PreviousExecutorRunHook = ExecutorRun_hook;
	ExecutorRun_hook = PgShardExecutorRun;

	PreviousExecutorFinishHook = ExecutorFinish_hook;
	ExecutorFinish_hook = PgShardExecutorFinish;

	PreviousExecutorEndHook = ExecutorEnd_hook;
	ExecutorEnd_hook = PgShardExecutorEnd;

	DefineCustomBoolVariable("pg_shard.use_citusdb_select_logic",
							 "Informs pg_shard to use CitusDB's select logic.",
							 NULL, &UseCitusDBSelectLogic, false, PGC_USERSET, 0,
							 NULL, NULL, NULL);
}


/*
 * _PG_fini is called when the module is unloaded. This function uninstalls the
 * extension's hooks.
 */
void
_PG_fini(void)
{
	planner_hook = PreviousPlannerHook;
	ExecutorStart_hook = PreviousExecutorStartHook;
	ExecutorRun_hook = PreviousExecutorRunHook;
	ExecutorFinish_hook = PreviousExecutorFinishHook;
	ExecutorEnd_hook = PreviousExecutorEndHook;
}


/*
 * PgShardPlannerHook implements custom planner logic to plan queries involving
 * distributed tables. It first calls the standard planner to perform common
 * mutations and normalizations on the query and retrieve the "normal" planned
 * statement for the query. Further functions actually produce the distributed
 * plan should one be necessary.
 */
static PlannedStmt *
PgShardPlannerHook(Query *query, int cursorOptions, ParamListInfo boundParams)
{
	PlannedStmt *plannedStatement = NULL;
	PlannerType plannerType = DeterminePlannerType(query);

	if (plannerType == PLANNER_TYPE_PG_SHARD)
	{
		DistributedPlan *distributedPlan = NULL;
		Query *distributedQuery = copyObject(query);

		/* call standard planner first to have Query transformations performed */
		plannedStatement = standard_planner(distributedQuery, cursorOptions,
											boundParams);

		ErrorIfQueryNotSupported(distributedQuery);

		distributedPlan = PlanDistributedQuery(distributedQuery);
		plannedStatement->planTree = (Plan *) distributedPlan;
	}
	else if (plannerType == PLANNER_TYPE_CITUSDB)
	{
		Assert(PreviousPlannerHook != NULL);
		plannedStatement = PreviousPlannerHook(query, cursorOptions, boundParams);
	}
	else if (plannerType == PLANNER_TYPE_POSTGRES)
	{
		if (PreviousPlannerHook != NULL)
		{
			plannedStatement = PreviousPlannerHook(query, cursorOptions, boundParams);
		}
		else
		{
			plannedStatement = standard_planner(query, cursorOptions, boundParams);
		}
	}
	else
	{
		ereport(ERROR, (errmsg("unknown planner type: %d", plannerType)));
	}

	return plannedStatement;
}


/*
 * DeterminePlannerType chooses the appropriate planner to use in order to plan
 * the given query.
 */
static PlannerType
DeterminePlannerType(Query *query)
{
	PlannerType plannerType = PLANNER_INVALID_FIRST;
	Oid distributedTableId = ExtractFirstDistributedTableId(query);

	if (query->commandType == CMD_SELECT && UseCitusDBSelectLogic)
	{
		plannerType = PLANNER_TYPE_CITUSDB;
	}
	else if (OidIsValid(distributedTableId))
	{
		plannerType = PLANNER_TYPE_PG_SHARD;
	}
	else
	{
		plannerType = PLANNER_TYPE_POSTGRES;
	}

	return plannerType;
}


/*
 * ExtractFirstDistributedTableId takes a given query, and finds the relationId
 * for the first distributed table in that query. If the function cannot find a
 * distributed table, it returns InvalidOid.
 */
static Oid
ExtractFirstDistributedTableId(Query *query)
{
	List *rangeTableList = NIL;
	ListCell *rangeTableCell = NULL;
	Oid distributedTableId = InvalidOid;

	/* extract range table entries */
	ExtractRangeTableEntryWalker((Node *) query, &rangeTableList);

	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);

		if (IsDistributedTable(rangeTableEntry->relid))
		{
			distributedTableId = rangeTableEntry->relid;
			break;
		}
	}

	return distributedTableId;
}


/*
 * ExtractRangeTableEntryWalker walks over a query tree, and finds all range
 * table entries that are plain relations or values scans. For recursing into
 * the query tree, this function uses the query tree walker since the expression
 * tree walker doesn't recurse into sub-queries.
 */
static bool
ExtractRangeTableEntryWalker(Node *node, List **rangeTableList)
{
	bool walkIsComplete = false;
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, RangeTblEntry))
	{
		RangeTblEntry *rangeTable = (RangeTblEntry *) node;
		if (rangeTable->rtekind == RTE_RELATION || rangeTable->rtekind == RTE_VALUES)
		{
			(*rangeTableList) = lappend(*rangeTableList, rangeTable);
		}
	}
	else if (IsA(node, Query))
	{
		walkIsComplete = query_tree_walker((Query *) node, ExtractRangeTableEntryWalker,
										   rangeTableList, QTW_EXAMINE_RTES);
	}
	else
	{
		walkIsComplete = expression_tree_walker(node, ExtractRangeTableEntryWalker,
												rangeTableList);
	}

	return walkIsComplete;
}


/*
 * ErrorIfQueryNotSupported checks if the query contains unsupported features,
 * and errors out if it does.
 */
static void
ErrorIfQueryNotSupported(Query *queryTree)
{
	List *rangeTableList = NIL;
	ListCell *rangeTableCell = NULL;
	bool hasValuesScan = false;
	uint32 queryTableCount = 0;

	CmdType commandType = queryTree->commandType;
	if (commandType != CMD_INSERT && commandType != CMD_SELECT)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("unsupported query type: %d", commandType)));
	}

	/* extract range table entries */
	ExtractRangeTableEntryWalker((Node *) queryTree, &rangeTableList);

	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);
		if (rangeTableEntry->rtekind == RTE_RELATION)
		{
			queryTableCount++;
		}
		else if (rangeTableEntry->rtekind == RTE_VALUES)
		{
			hasValuesScan = true;
		}
	}

	/* reject queries which involve joins */
	if (queryTableCount != 1)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot perform distributed planning on this query"),
						errdetail("Joins are currently unsupported")));
	}

	/* reject queries which involve multi-row inserts */
	if (hasValuesScan)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("multi-row INSERTs to distributed tables "
							   "are not supported")));
	}

	/* reject queries with a returning list */
	if (list_length(queryTree->returningList) > 0)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot plan sharded INSERT that uses a "
							   "RETURNING clause")));
	}
}


/*
 * PlanDistributedQuery is the main entry point when planning a query involving
 * a distributed table. The function prunes the shards for the table in the
 * query based on the query's restriction qualifiers, and then builds a
 * distributed plan with those shards.
 */
static DistributedPlan *
PlanDistributedQuery(Query *query)
{
	DistributedPlan *distributedPlan = NULL;
	Oid distributedTableId = ExtractFirstDistributedTableId(query);

	List *restrictClauseList = QueryRestrictList(query);
	List *shardList = LoadShardList(distributedTableId);
	List *prunedShardList = PruneShardList(distributedTableId, restrictClauseList,
										   shardList);

	distributedPlan = BuildDistributedPlan(query, prunedShardList);

	return distributedPlan;
}


/*
 * QueryRestrictList returns the restriction clauses for the query. For a SELECT
 * statement these are the where-clause expressions. For INSERT statements we
 * build an equality clause based on the partition-column and its supplied
 * insert value.
 */
static List *
QueryRestrictList(Query *query)
{
	List *queryRestrictList = NIL;

	if (query->commandType == CMD_INSERT)
	{
		/* build equality expression based on partition column value for row */
		Oid distributedTableId = ExtractFirstDistributedTableId(query);
		Var *partitionColumn = PartitionColumn(distributedTableId);
		Const *partitionValue = ExtractPartitionValue(query, partitionColumn);

		OpExpr *equalityExpr = MakeOpExpression(partitionColumn, BTEqualStrategyNumber);

		Node *rightOp = get_rightop((Expr *) equalityExpr);
		Const *rightConst = (Const *) rightOp;
		Assert(IsA(rightOp, Const));

		rightConst->constvalue = partitionValue->constvalue;
		rightConst->constisnull = partitionValue->constisnull;
		rightConst->constbyval = partitionValue->constbyval;

		queryRestrictList = list_make1(equalityExpr);
	}
	else if (query->commandType == CMD_SELECT)
	{
		query_tree_walker(query, ExtractFromExpressionWalker, &queryRestrictList, 0);
	}

	return queryRestrictList;
}


/*
 * ExtractPartitionValue extracts the partition column value from a the target
 * of a modification command. If a partition value is not a constant, is NULL,
 * or is missing altogether, this function throws an error.
 */
static Const *
ExtractPartitionValue(Query *query, Var *partitionColumn)
{
	Const *partitionValue = NULL;
	TargetEntry *targetEntry = get_tle_by_resno(query->targetList,
												partitionColumn->varattno);
	if (targetEntry != NULL)
	{
		if (!IsA(targetEntry->expr, Const))
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot plan INSERT to a distributed table "
									"using a non-constant partition column value")));
		}

		partitionValue = (Const *) targetEntry->expr;
	}

	if (partitionValue == NULL || partitionValue->constisnull)
	{
		ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("cannot plan INSERT using row with NULL value "
							   "in partition column")));
	}

	return partitionValue;
}


/*
 * ExtractFromExpressionWalker walks over a FROM expression, and finds all
 * explicit qualifiers in the expression.
 */
static bool
ExtractFromExpressionWalker(Node *node, List **qualifierList)
{
	bool walkIsComplete = false;
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, FromExpr))
	{
		FromExpr *fromExpression = (FromExpr *) node;
		List *fromQualifierList = (List *) fromExpression->quals;
		(*qualifierList) = list_concat(*qualifierList, fromQualifierList);
	}

	walkIsComplete = expression_tree_walker(node, ExtractFromExpressionWalker,
											(void *) qualifierList);

	return walkIsComplete;
}


/*
 * BuildDistributedPlan simply creates the DistributedPlan instance from the
 * provided query and shard interval list.
 */
static DistributedPlan *
BuildDistributedPlan(Query *query, List *shardIntervalList)
{
	ListCell *shardIntervalCell = NULL;
	List *taskList = NIL;
	TupleDesc returnTupleDescriptor = NULL;
	DistributedPlan *distributedPlan = palloc0(sizeof(DistributedPlan));
	distributedPlan->plan.type = (NodeTag) T_DistributedPlan;

	/* construct a descriptor which describes the tuples we return */
	returnTupleDescriptor = ExecTypeFromTL(query->targetList, false);

	foreach(shardIntervalCell, shardIntervalList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(shardIntervalCell);
		int64 shardId = shardInterval->id;
		List *finalizedPlacementList = LoadFinalizedShardPlacementList(shardId);
		Task *task = NULL;

		StringInfo queryString = makeStringInfo();
		deparse_shard_query(query, shardId, queryString);

		task = (Task *) palloc0(sizeof(Task));
		task->queryString = queryString;
		task->taskPlacementList = finalizedPlacementList;

		taskList = lappend(taskList, task);
	}

	distributedPlan->taskList = taskList;
	distributedPlan->tupleDescriptor = returnTupleDescriptor;

	return distributedPlan;
}


/*
 * PgShardExecutorStart implements the ExecutorStart hook to enable distributed
 * execution. The function currently sets up the executor state needed in later
 * parts of the execution flow.
 */
static void
PgShardExecutorStart(QueryDesc *queryDesc, int eflags)
{
	PlannedStmt *planStatement = queryDesc->plannedstmt;
	NodeTag nodeType = nodeTag(planStatement->planTree);

	if (nodeType == T_DistributedPlan)
	{
		DistributedPlan *distributedPlan = (DistributedPlan *) planStatement->planTree;
		TupleDesc tupleDescriptor = distributedPlan->tupleDescriptor;

		/* build empty executor state to obtain per-query memory context */
		EState *executorState = CreateExecutorState();
		queryDesc->estate = executorState;
		queryDesc->tupDesc = tupleDescriptor;
	}
	else
	{
		/* this isn't a query pg_shard handles: use previous hook or standard */
		if (PreviousExecutorStartHook != NULL)
		{
			PreviousExecutorStartHook(queryDesc, eflags);
		}
		else
		{
			standard_ExecutorStart(queryDesc, eflags);
		}
	}
}


/*
 * PgShardExecutorRun implements the ExecutorRun hook to enable distributed
 * execution. The function checks if the plan is a distributed plan, and if so
 * it runs the pgShard distributed query execution logic. If not the function
 * defers to the previous execution hooks.
 */
static void
PgShardExecutorRun(QueryDesc *queryDesc, ScanDirection direction, long count)
{
	PlannedStmt *planStatement = queryDesc->plannedstmt;
	NodeTag nodeType = nodeTag(planStatement->planTree);

	if (nodeType == T_DistributedPlan)
	{
		DistributedPlan *distributedPlan = (DistributedPlan *) planStatement->planTree;
		EState *executorState = queryDesc->estate;
		CmdType operation = queryDesc->operation;
		DestReceiver *destination = queryDesc->dest;

		MemoryContext oldcontext = MemoryContextSwitchTo(executorState->es_query_cxt);

		if (ScanDirectionIsNoMovement(direction))
		{
			ereport(ERROR, (errmsg("unsupported no movement scan direction")));
		}

		if (operation == CMD_SELECT)
		{
			ExecuteDistributedSelect(distributedPlan, executorState, destination);
		}

		MemoryContextSwitchTo(oldcontext);
	}
	else
	{
		/* this isn't a query pg_shard handles: use previous hook or standard */
		if (PreviousExecutorRunHook != NULL)
		{
			PreviousExecutorRunHook(queryDesc, direction, count);
		}
		else
		{
			standard_ExecutorRun(queryDesc, direction, count);
		}
	}
}


/*
 * ExecuteDistributedSelect executes the remote select query and sends the
 * resultant tuples to the given destination receiver. The function first
 * executes the query remotely and stores the results into a tuple store. The
 * function then iterates over the tuple store and sends the tuples to the
 * destination. If the query fails on a given placement, the function attempts
 * it on its replica.
 */
static void
ExecuteDistributedSelect(DistributedPlan *distributedPlan, EState *executorState,
						 DestReceiver *destination)
{
	Task *task = NULL;
	List *taskList = distributedPlan->taskList;
	List *shardPlacementList = NIL;
	ListCell *shardPlacementCell = NULL;

	TupleDesc tupleDescriptor = distributedPlan->tupleDescriptor;
	TupleTableSlot *tupleTableSlot = MakeSingleTupleTableSlot(tupleDescriptor);
	bool forwardDirection = true;
	Tuplestorestate *tupleStore = NULL;

	if (list_length(taskList) != 1)
	{
		ereport(ERROR, (errmsg("cannot execute select over multiple shards")));
	}

	task = (Task *) linitial(taskList);
	shardPlacementList = task->taskPlacementList;

	/*
	 * Try to run the query to completion on one placement and store the results
	 * in a tuple store. If the query fails attempt the query on the next
	 * placement.
	 */
	foreach(shardPlacementCell, shardPlacementList)
	{
		ShardPlacement *shardPlacement = (ShardPlacement *) lfirst(shardPlacementCell);
		tupleStore = ExecuteRemoteQuery(shardPlacement->nodeName,
										shardPlacement->nodePort,
										task->queryString,
										tupleDescriptor);

		/* if the query succeeded break out of the loop */
		if (tupleStore != NULL)
		{
			break;
		}
	}

	if (tupleStore == NULL)
	{
		ereport(ERROR, (errmsg("unable to execute remote query")));
	}

	/* startup the tuple receiver */
	(*destination->rStartup) (destination, CMD_SELECT, tupleDescriptor);

	/* loop and retrieve all the tuples from the tuplestore */
	while (tuplestore_gettupleslot(tupleStore, forwardDirection, false, tupleTableSlot))
	{
		(*destination->receiveSlot) (tupleTableSlot, destination);
		executorState->es_processed++;

		ExecClearTuple(tupleTableSlot);
	}

	/* shutdown the tuple receiver and cleanup the tupleStore */
	(*destination->rShutdown) (destination);
	tuplestore_end(tupleStore);
}


/*
 * ExecuteRemoteQuery takes the given query and attempts to execute it on the
 * given node and port combination. If the query succeeds the function stores
 * the results in a tuple store and returns this store. Note that the caller
 * is expected to free the tuple store after using it.
 */
static Tuplestorestate *
ExecuteRemoteQuery(char *nodeName, int32 nodePort, StringInfo query,
				   TupleDesc tupleDescriptor)
{
	uint32 rowIndex = 0;
	uint32 rowCount = 0;
	uint32 fieldIndex = 0;
	uint32 fieldCount = 0;
	char **values = NULL;
	bool randomAccess = false;
	bool interXact = false;
	Tuplestorestate *tupleStore = NULL;
	AttInMetadata *attributeInMetadata = NULL;
	PGresult *result = NULL;

	PGconn *connection = GetConnection(nodeName, nodePort);
	if (connection == NULL)
	{
		ereport(WARNING, (errmsg("could not connect to \"%s:%u\"",
								 nodeName, nodePort)));
		return NULL;
	}

	/* initialize the tuplestore state and attribute input information */
	tupleStore = tuplestore_begin_heap(randomAccess, interXact, work_mem);
	attributeInMetadata = TupleDescGetAttInMetadata(tupleDescriptor);

	/* now execute the query on the remote node */
	result = PQexec(connection, query->data);
	if (PQresultStatus(result) != PGRES_TUPLES_OK)
	{
		ereport(WARNING, (errmsg("could not run query on \"%s:%u\"",
								 nodeName, nodePort)));
		tuplestore_end(tupleStore);
		return NULL;
	}

	rowCount = PQntuples(result);
	fieldCount = PQnfields(result);
	values = (char **) palloc(fieldCount * sizeof(char *));

	/* now create tuples for each row and store them in the tuple store */
	for (rowIndex = 0; rowIndex < rowCount; rowIndex++)
	{
		HeapTuple resultTuple = NULL;

		for (fieldIndex = 0; fieldIndex < fieldCount; fieldIndex++)
		{
			if (PQgetisnull(result, rowIndex, fieldIndex))
			{
				values[fieldIndex] = NULL;
			}
			else
			{
				values[fieldIndex] = PQgetvalue(result, rowIndex, fieldIndex);
			}
		}

		resultTuple = BuildTupleFromCStrings(attributeInMetadata, values);
		tuplestore_puttuple(tupleStore, resultTuple);
	}

	return tupleStore;
}


/*
 * PgShardExecutorFinish implements the ExecutorFinish hook to enable
 * distributed execution.
 */
static void
PgShardExecutorFinish(QueryDesc *queryDesc)
{
	PlannedStmt *planStatement = queryDesc->plannedstmt;
	NodeTag nodeType = nodeTag(planStatement->planTree);

	if (nodeType == T_DistributedPlan)
	{
		EState *executorState = queryDesc->estate;
		executorState->es_finished = true;
	}
	else
	{
		/* this isn't a query pg_shard handles: use previous hook or standard */
		if (PreviousExecutorFinishHook != NULL)
		{
			PreviousExecutorFinishHook(queryDesc);
		}
		else
		{
			standard_ExecutorFinish(queryDesc);
		}
	}
}


/*
 * PgShardExecutorEnd implements the ExecutorEnd hook to enable distributed
 * execution. The function currently performs cleanup of the executor state and
 * the query descriptor.
 */
static void
PgShardExecutorEnd(QueryDesc *queryDesc)
{
	PlannedStmt *planStatement = queryDesc->plannedstmt;
	NodeTag nodeType = nodeTag(planStatement->planTree);

	if (nodeType == T_DistributedPlan)
	{
		FreeExecutorState(queryDesc->estate);

		/* reset queryDesc fields that no longer point to anything */
		queryDesc->tupDesc = NULL;
		queryDesc->estate = NULL;
		queryDesc->planstate = NULL;
		queryDesc->totaltime = NULL;
	}
	else
	{
		/* this isn't a query pg_shard handles: use previous hook or standard */
		if (PreviousExecutorEndHook != NULL)
		{
			PreviousExecutorEndHook(queryDesc);
		}
		else
		{
			standard_ExecutorEnd(queryDesc);
		}
	}
}
