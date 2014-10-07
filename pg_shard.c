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
#include "libpq-fe.h"
#include "pg_config.h"
#include "postgres_ext.h"

#include "pg_shard.h"
#include "connection.h"
#include "create_shards.h"
#include "distribution_metadata.h"
#include "prune_shard_list.h"
#include "ruleutils.h"

#include <stddef.h>

#include "access/sdir.h"
#include "access/skey.h"
#include "access/xact.h"
#include "executor/execdesc.h"
#include "executor/executor.h"
#include "executor/instrument.h"
#include "nodes/execnodes.h"
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
#include "utils/builtins.h"
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
static void PgShardExecutorStart(QueryDesc *queryDesc, int eflags);
void PgShardExecutorRun(QueryDesc *queryDesc, ScanDirection direction, long count);
static bool ExtractRangeTableEntryWalker(Node *node, List **rangeTableList);
static void ErrorIfQueryNotSupported(Query *queryTree);
static DistributedPlan * PlanDistributedQuery(Query *query);
static List * QueryRestrictList(Query *query);
static int32 ExecuteDistributedModify(DistributedPlan *distributedPlan);
static Const * ExtractPartitionValue(Query *query, Var *partitionColumn);
static bool ExtractFromExpressionWalker(Node *node, List **qualifierList);
static DistributedPlan * BuildDistributedPlan(Query *query, List *shardIntervalList);
static bool IsPgShardPlan(PlannedStmt *plannedStmt);
static void AcquireExecutorShardLocks(PlannedStmt *plannedStatement);
static int CompareTasksByShardId(const void *leftElement, const void *rightElement);


/* declarations for dynamic loading */
PG_MODULE_MAGIC;


/* saved hook values in case of unload */
static planner_hook_type PreviousPlannerHook = NULL;
static ExecutorStart_hook_type PreviousExecutorStartHook = NULL;
static ExecutorRun_hook_type PreviousExecutorRunHook = NULL;


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
	ExecutorRun_hook = PreviousExecutorRunHook;
	ExecutorStart_hook = PreviousExecutorStartHook;
	planner_hook = PreviousPlannerHook;
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
		distributedPlan->originalPlan = plannedStatement->planTree;
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
 * PgShardExecutorStart sets up the queryDesc so even distributed queries can benefit
 * from the standard ExecutorStart logic. After that hook finishes its setup work, this
 * function moves the special distributed plan back into place for our run hook.
 */
static void
PgShardExecutorStart(QueryDesc *queryDesc, int eflags)
{
	PlannedStmt *plannedStatement = queryDesc->plannedstmt;
	bool pgShardExecution = IsPgShardPlan(plannedStatement);
	DistributedPlan *distributedPlan = NULL;

	if (pgShardExecution)
	{
		Plan *originalPlan = NULL;
		bool topLevel = true;

		distributedPlan = (DistributedPlan *) plannedStatement->planTree;
		originalPlan = distributedPlan->originalPlan;

		/* disallow transactions and triggers during distributed commands */
		PreventTransactionChain(topLevel, "distributed commands");
		eflags |= EXEC_FLAG_SKIP_TRIGGERS;

		/* swap in original (local) plan for compatibility with standard start hook */
		plannedStatement->planTree = originalPlan;
	}

	/* call the next hook in the chain or the standard one, if no other hook was set */
	if (PreviousExecutorStartHook != NULL)
	{
		PreviousExecutorStartHook(queryDesc, eflags);
	}
	else
	{
		standard_ExecutorStart(queryDesc, eflags);
	}

	if (pgShardExecution)
	{
		/* swap back to the distributed plan for rest of query */
		plannedStatement->planTree = (Plan *) distributedPlan;

		AcquireExecutorShardLocks(plannedStatement);
	}
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
 * PgShardExecutorRun actually runs a distributed plan, if any.
 */
void
PgShardExecutorRun(QueryDesc *queryDesc, ScanDirection direction, long count)
{
	PlannedStmt *plannedStatement = queryDesc->plannedstmt;
	bool pgShardExecution = IsPgShardPlan(plannedStatement);

	if (pgShardExecution)
	{
		EState *estate = queryDesc->estate;
		CmdType operation = queryDesc->operation;
		DistributedPlan *plan = (DistributedPlan *) plannedStatement->planTree;
		MemoryContext oldcontext = NULL;

		Assert(estate != NULL);
		Assert(!(estate->es_top_eflags & EXEC_FLAG_EXPLAIN_ONLY));

		oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

		if (queryDesc->totaltime != NULL)
		{
			InstrStartNode(queryDesc->totaltime);
		}

		if (!ScanDirectionIsNoMovement(direction))
		{
			if (operation == CMD_INSERT)
			{
				int32 affectedRowCount = ExecuteDistributedModify(plan);
				estate->es_processed = affectedRowCount;
			}
		}

		if (queryDesc->totaltime != NULL)
		{
			InstrStopNode(queryDesc->totaltime, estate->es_processed);
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
 * ExecuteDistributedModify is the main entry point for modifying distributed
 * tables. A distributed modification is successful if any placement of the
 * distributed table is successful. ExecuteDistributedModify returns the number
 * of modified rows in that case and errors in all others. This function will
 * also generate warnings for individual placement failures.
 */
static int32
ExecuteDistributedModify(DistributedPlan *plan)
{
	int32 affectedTupleCount = -1;
	Task *task = (Task *) linitial(plan->taskList);
	ListCell *taskPlacementCell = NULL;
	List *failedPlacementList = NIL;
	ListCell *failedPlacementCell = NULL;

	/* we only support a single modification to a single shard */
	if (list_length(plan->taskList) != 1)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot plan INSERT to more than one shard")));
	}

	foreach(taskPlacementCell, task->taskPlacementList)
	{
		ShardPlacement *taskPlacement = (ShardPlacement *) lfirst(taskPlacementCell);
		char *nodeName = taskPlacement->nodeName;
		int32 nodePort = taskPlacement->nodePort;

		PGconn *connection = NULL;
		PGresult *result = NULL;
		char *currentAffectedTupleString = NULL;
		int32 currentAffectedTupleCount = -1;

		Assert(taskPlacement->shardState == STATE_FINALIZED);

		connection = GetConnection(nodeName, nodePort);
		if (connection == NULL)
		{
			ereport(WARNING, (errmsg("could not connect to %s:%d", nodeName, nodePort)));
			failedPlacementList = lappend(failedPlacementList, taskPlacement);

			continue;
		}

		result = PQexec(connection, task->queryString->data);
		if (PQresultStatus(result) != PGRES_COMMAND_OK)
		{
			ReportRemoteError(connection, result);
			failedPlacementList = lappend(failedPlacementList, taskPlacement);

			continue;
		}

		currentAffectedTupleString = PQcmdTuples(result);
		currentAffectedTupleCount = pg_atoi(currentAffectedTupleString, sizeof(int32), 0);

		if ((affectedTupleCount == -1) ||
			(affectedTupleCount == currentAffectedTupleCount))
		{
			affectedTupleCount = currentAffectedTupleCount;
		}
		else
		{
			ereport(WARNING, (errmsg("modified %d tuples, but expected to modify %d",
									 currentAffectedTupleCount, affectedTupleCount),
							  errdetail("modified placement on %s:%d",
									    nodeName, nodePort)));
		}

		PQclear(result);
	}

	/* if all placements failed, error out */
	if (list_length(failedPlacementList) == list_length(task->taskPlacementList))
	{
		ereport(ERROR, (errmsg("could not modify any active placements")));
	}

	/* otherwise, mark failed placements as inactive: they're stale */
	foreach(failedPlacementCell, failedPlacementList)
	{
		ShardPlacement *failedPlacement = (ShardPlacement *) lfirst(failedPlacementCell);

		DeleteShardPlacementRow(failedPlacement->id);
		InsertShardPlacementRow(failedPlacement->id, failedPlacement->shardId,
								STATE_INACTIVE, failedPlacement->nodeName,
								failedPlacement->nodePort);
	}

	return affectedTupleCount;
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
	DistributedPlan *distributedPlan = palloc0(sizeof(DistributedPlan));
	distributedPlan->plan.type = (NodeTag) T_DistributedPlan;

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
		task->shardId = shardId;

		taskList = lappend(taskList, task);
	}

	distributedPlan->taskList = taskList;

	return distributedPlan;
}


/*
 * IsPgShardPlan determines whether the provided plannedStmt contains a plan
 * suitable for execution by PgShard.
 */
static bool
IsPgShardPlan(PlannedStmt *plannedStmt)
{
	Plan *plan = plannedStmt->planTree;
	NodeTag nodeTag = nodeTag(plan);
	bool isPgShardPlan = ((DistributedNodeTag) nodeTag == T_DistributedPlan);

	return isPgShardPlan;
}


/*
 * AcquireExecutorShardLocks: acquire shard locks needed for execution of a
 * distributed plan. Assumes plannedStatement->planTree is a DistributedPlan.
 */
static void
AcquireExecutorShardLocks(PlannedStmt *plannedStatement)
{
	CmdType operation = plannedStatement->commandType;
	DistributedPlan *distributedPlan = (DistributedPlan *) plannedStatement->planTree;
	List *shardIdSortedTaskList = NIL;
	ListCell *taskCell = NULL;
	LOCKMODE lockMode = NoLock;

	switch (operation)
	{
		case CMD_SELECT:
		{
			/* no locks needed */
			break;
		}
		case CMD_INSERT:
		{
			lockMode = ShareLock;
			break;
		}
		case CMD_DELETE:
		case CMD_UPDATE:
		{
			lockMode = ExclusiveLock;
			break;
		}
		default:
		{
			ereport(ERROR, (errmsg("unrecognized operation code: %d", (int) operation)));
			break;
		}
	}

	if (lockMode == NoLock)
	{
		return;
	}

	shardIdSortedTaskList = SortList(distributedPlan->taskList, CompareTasksByShardId);

	foreach(taskCell, shardIdSortedTaskList)
	{
		Task *task = (Task *) lfirst(taskCell);
		int64 shardId = task->shardId;

		LockShard(shardId, lockMode);
	}
}


/* Helper function to compare two tasks using their shardIds. */
static int
CompareTasksByShardId(const void *leftElement, const void *rightElement)
{
	const Task *leftTask = *((const Task **) leftElement);
	const Task *rightTask = *((const Task **) rightElement);
	int64 leftShardId = leftTask->shardId;
	int64 rightShardId = rightTask->shardId;

	/* we compare 64-bit integers, instead of casting their difference to int */
	if (leftShardId > rightShardId)
	{
		return 1;
	}
	else if (leftShardId < rightShardId)
	{
		return -1;
	}
	else
	{
		return 0;
	}
}
