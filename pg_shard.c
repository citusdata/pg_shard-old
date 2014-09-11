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
#include "partition_pruning.h"
#include "ruleutils.h"

#include <stddef.h>

#include "access/skey.h"
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


/* informs pg_shard that its running on a CitusDB installation */
bool CitusDBInstalled = false;


/* local forward declarations */
static PlannedStmt * PgShardPlannerHook(Query *parse, int cursorOptions,
										ParamListInfo boundParams);
static bool NeedsDistributedPlanning(Query *queryTree);
static bool ExtractRangeTableRelationWalker(Node *node, List **rangeTableList);
static void ErrorIfQueryNotSupported(Query *queryTree);
static bool HasUnsupportedJoinWalker(Node *node, void *context);
static DistributedPlan * PlanDistributedQuery(Query *query);
static Oid QueryRelationId(Query *query);
static List * QueryRestrictList(Query *query);
static Const * ExtractPartitionValue(Query *query, Var *partitionColumn);
static void UpdateRightOpConst(const OpExpr *clause, Const *constNode);
static List * WhereClauseList(FromExpr *fromExpr);
static bool ExtractFromExpressionWalker(Node *node, List **qualifierList);
static DistributedPlan * BuildDistributedPlan(Query *query, List *targetShardList);
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

	DefineCustomBoolVariable("pg_shard.citusDBInstalled",
							 "Informs pg_shard that it is running on CitusDB installation.",
							 NULL, &CitusDBInstalled, false, PGC_SUSET, 0, NULL,
							 NULL, NULL);
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
	CmdType cmdType = query->commandType;
	bool needsDistributedPlanning = NeedsDistributedPlanning(query);

	/* if a select query and running on CitusDB, defer to CitusDB's planner */
	if ((!needsDistributedPlanning) || (cmdType == CMD_SELECT && CitusDBInstalled))
	{
		if (PreviousPlannerHook != NULL)
		{
			plannedStatement = PreviousPlannerHook(query, cursorOptions, boundParams);
		}
		else
		{
			plannedStatement = standard_planner(query, cursorOptions, boundParams);
		}

		return plannedStatement;
	}

	if (needsDistributedPlanning)
	{
		DistributedPlan *distributedPlan = NULL;
		Query *distributedQuery = copyObject(query);

		/* call standard planner on copy to have Query transformations performed */
		plannedStatement = standard_planner(distributedQuery, cursorOptions,
											boundParams);

		if (cmdType == CMD_SELECT)
		{
			ErrorIfQueryNotSupported(distributedQuery);			
		}

		distributedPlan = PlanDistributedQuery(distributedQuery);
		plannedStatement->planTree = (Plan *) distributedPlan;
	}

	return plannedStatement;
}


/*
 * NeedsDistributedPlanning checks if the passed in Query is an INSERT or SELECT
 * command running on partitioned relations. If it is, we start distributed
 * planning.
 *
 * This function throws an error if the query represents a multi-row INSERT or
 * mixes distributed and local tables.
 */
static bool
NeedsDistributedPlanning(Query *queryTree)
{
	List *rangeTableList = NIL;
	ListCell *rangeTableCell = NULL;
	bool hasLocalRelation = false;
	bool hasDistributedRelation = false;
	bool hasValuesScan = false;

	if (queryTree->commandType != CMD_INSERT && queryTree->commandType != CMD_SELECT)
	{
		return false;
	}

	/* extract range table entries for simple relations only */
	ExtractRangeTableRelationWalker((Node *) queryTree, &rangeTableList);

	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);

		if(rangeTableEntry->rtekind == RTE_VALUES)
		{
			hasValuesScan = true;
		}
		else if (TableIsDistributed(rangeTableEntry->relid))
		{
			hasDistributedRelation = true;
		}
		else
		{
			hasLocalRelation = true;
		}
	}

	if (hasDistributedRelation)
	{
		/* users can't mix local and distributed relations in one query */
		if (hasLocalRelation)
		{
			ereport(ERROR, (errmsg("cannot plan queries that include both regular and "
								   "partitioned relations")));
		}

		if (hasValuesScan)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("multi-row INSERTs to distributed tables "
								   "are not supported")));
		}
	}

	return hasDistributedRelation;
}


/*
 * ExtractRangeTableRelationWalker walks over a query tree, and finds all range
 * table entries that are plain relations or values scans. For recursing into
 * the query tree, this function uses the query tree walker since the expression
 * tree walker doesn't recurse into sub-queries.
 *
 * Values scans are not supported, but there is no field on the Query that can
 * be easily checked to detect them, so we collect them here and let the logic
 * in NeedsDistributedPlanning sort it out.
 */
static bool
ExtractRangeTableRelationWalker(Node *node, List **rangeTableList)
{
	bool walkerResult = false;
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
		walkerResult = query_tree_walker((Query *) node, ExtractRangeTableRelationWalker,
										 rangeTableList, QTW_EXAMINE_RTES);
	}
	else
	{
		walkerResult = expression_tree_walker(node, ExtractRangeTableRelationWalker,
											  rangeTableList);
	}

	return walkerResult;
}


/*
 * ErrorIfQueryNotSupported checks that we can perform distributed planning for
 * the given query. The checks in this function will be removed as we support
 * more functionality in our distributed planning.
 */
static void
ErrorIfQueryNotSupported(Query *queryTree)
{
	CmdType commandType = queryTree->commandType;

	if (commandType == CMD_INSERT)
	{
		/* reject queries with a returning list */
		if (list_length(queryTree->returningList) > 0)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot plan sharded INSERT that uses a "
								   "RETURNING clause")));
		}
	}

	if (commandType == CMD_SELECT)
	{
		bool hasUnsupportedJoin = HasUnsupportedJoinWalker((Node *) queryTree->jointree,
														   NULL);

		/* reject the query if it contains joins */
		if (hasUnsupportedJoin)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot perform distributed planning on this query"),
							errdetail("Join types other than inner joins are currently"
									  " unsupported")));
		}
	}
}


/*
 * HasUnsupportedJoinWalker returns true if the query contains a join between
 * multiple tables.
 */
static bool
HasUnsupportedJoinWalker(Node *node, void *context)
{
	bool hasUnsupportedJoin = false;
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, JoinExpr))
	{
		hasUnsupportedJoin = true;
	}

	if (!hasUnsupportedJoin)
	{
		hasUnsupportedJoin = expression_tree_walker(node, HasUnsupportedJoinWalker,
													NULL);
	}

	return hasUnsupportedJoin;
}


/*
 * PlanDistributedQuery is the main entry point when planning a query on a
 * distributed table. A DistributedPlan for the query in question is returned.
 */
static DistributedPlan *
PlanDistributedQuery(Query *query)
{
	DistributedPlan *distributedPlan = NULL;
	Oid distributedTableId = QueryRelationId(query);

	List *restrictClauseList = QueryRestrictList(query);
	List *shardList = LoadShardList(distributedTableId);
	List *prunedShardList = PruneShardList(distributedTableId, restrictClauseList,
										   shardList);

	if (list_length(prunedShardList) != 1)
	{
		ereport(ERROR, (errmsg("cannot run query across multiple shards")));
	}

	distributedPlan = BuildDistributedPlan(query, prunedShardList);

	return distributedPlan;
}


/*
 * QueryRelationId returns the relationId of the table on which this query will
 * be executed. Note that the function assumes the query contains only a single
 * table.
 */
static Oid
QueryRelationId(Query *query)
{
	List *rangeTableList = NIL;
	RangeTblEntry *rangeTableEntry = NULL;

	/* extract range table entries for simple relations only */
	ExtractRangeTableRelationWalker((Node *) query, &rangeTableList);

	Assert(list_length(rangeTableList) == 1);
	rangeTableEntry = (RangeTblEntry *) linitial(rangeTableList);

	return rangeTableEntry->relid;
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
		Oid distributedTableId = QueryRelationId(query);
		Var *partitionColumn = PartitionColumn(distributedTableId);
		Const *partitionValue = ExtractPartitionValue(query, partitionColumn);

		OpExpr *equalityExpr = MakeOpExpression(partitionColumn, BTEqualStrategyNumber);
		UpdateRightOpConst(equalityExpr, partitionValue);
		queryRestrictList = list_make1(equalityExpr);
	}

	if (query->commandType == CMD_SELECT)
	{
		queryRestrictList = WhereClauseList(query->jointree);
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
	Const *value = NULL;
	TargetEntry *targetEntry = get_tle_by_resno(query->targetList,
												partitionColumn->varattno);
	if (targetEntry != NULL)
	{
		if (!IsA(targetEntry->expr, Const))
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot plan sharded INSERT using a non-"
								   "constant partition column value")));
		}

		value = (Const *) targetEntry->expr;
	}

	if (value == NULL || value->constisnull)
	{
		ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("cannot plan INSERT using row with NULL value "
							   "in partition column")));
	}

	return value;
}


/*
 * UpdateRightOpValue updates the provided clause (in-place) by replacing its
 * right-hand side with the provided value.
 */
static void
UpdateRightOpConst(const OpExpr *clause, Const *constNode)
{
	Node *rightOp = get_rightop((Expr *)clause);
	Const *rightConst = NULL;

	Assert(IsA(rightOp, Const));

	rightConst = (Const *) rightOp;

	rightConst->constvalue = constNode->constvalue;
	rightConst->constisnull = constNode->constisnull;
	rightConst->constbyval = constNode->constbyval;
}


/*
 * WhereClauseList walks over the FROM expression in the query tree, and builds
 * a list of all clauses from the expression tree. The function checks for both
 * implicitly and explicitly defined clauses. Explicit clauses are expressed as
 * "SELECT ... FROM R1 INNER JOIN R2 ON R1.A = R2.A". Implicit joins differ in
 * that they live in the WHERE clause, and are expressed as "SELECT ... FROM
 * ... WHERE R1.a = R2.a".
 */
static List *
WhereClauseList(FromExpr *fromExpr)
{
	FromExpr *fromExprCopy = copyObject(fromExpr);
	List *whereExpressionList = NIL;
	List *fromExpressionList = NIL;
	List *combinedExpressionList = NIL;

	/* extract implicit and explicit qualifiers */
	fromExpressionList = NIL;
	whereExpressionList = (List *) fromExprCopy->quals;
	ExtractFromExpressionWalker((Node *) fromExprCopy->fromlist, &fromExpressionList);

	/* combine both qualifiers into a single list */
	combinedExpressionList = list_concat(fromExpressionList, whereExpressionList);

	return combinedExpressionList;
}


/*
 * ExtractFromExpressionWalker walks over a FROM expression, and finds all
 * explicit qualifiers in the expression. The function looks at join and from
 * expression nodes to find explicit qualifiers, and returns these qualifiers.
 */
static bool
ExtractFromExpressionWalker(Node *node, List **qualifierList)
{
	bool walkerResult = false;
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, JoinExpr))
	{
		JoinExpr *joinExpression = (JoinExpr *) node;
		List *joinQualifierList = (List *) joinExpression->quals;
		(*qualifierList) = list_concat(*qualifierList, joinQualifierList);
	}
	else if (IsA(node, FromExpr))
	{
		FromExpr *fromExpression = (FromExpr *) node;
		List *fromQualifierList = (List *) fromExpression->quals;
		(*qualifierList) = list_concat(*qualifierList, fromQualifierList);
	}

	walkerResult = expression_tree_walker(node, ExtractFromExpressionWalker,
										  (void *) qualifierList);

	return walkerResult;
}


/*
 * BuildDistributedPlan simply creates the DistributedPlan instance from the
 * provided query and target shard list.
 */
static DistributedPlan *
BuildDistributedPlan(Query *query, List *targetShardList)
{
	DistributedPlan *distributedPlan = makeDistNode(DistributedPlan);
	ListCell *targetShardCell = NULL;
	List *taskList = NIL;
	TupleDesc tupleDescriptor = ExecTypeFromTL(query->targetList, false);

	foreach(targetShardCell, targetShardList)
	{
		ShardInterval *targetShard = (ShardInterval *) lfirst(targetShardCell);
		List *finalizedPlacementList = LoadFinalizedShardPlacementList(targetShard->id);
		Task *task = NULL;

		StringInfo queryString = makeStringInfo();
		deparse_shard_query(query, targetShard->id, queryString);

		task = (Task *) palloc0(sizeof(Task));
		task->queryString = queryString;
		task->taskPlacementList = finalizedPlacementList;

		taskList = lappend(taskList, task);
	}

	distributedPlan->taskList = taskList;
	distributedPlan->tupleDescriptor = tupleDescriptor;

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

		/* extract information from the query descriptor and the query feature */
		CmdType operation = queryDesc->operation;
		DestReceiver *destination = queryDesc->dest;

		MemoryContext oldcontext = MemoryContextSwitchTo(executorState->es_query_cxt);

		/* XXX should this be an assert? */
		if (!ScanDirectionIsNoMovement(direction))
		{
			if (operation == CMD_SELECT)
			{
				ExecuteDistributedSelect(distributedPlan, executorState, destination);
			}
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
		ereport(ERROR, (errmsg("cannot execute multiple tasks")));
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
