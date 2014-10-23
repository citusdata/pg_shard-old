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
#include "create_shards.h"
#include "distribution_metadata.h"
#include "prune_shard_list.h"
#include "ruleutils.h"

#include <stddef.h>
#include <string.h>

#include "access/sdir.h"
#include "access/skey.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "executor/execdesc.h"
#include "executor/executor.h"
#include "executor/instrument.h"
#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "nodes/nodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "nodes/primnodes.h"
#include "optimizer/clauses.h"
#include "optimizer/planner.h"
#include "optimizer/var.h"
#include "parser/parsetree.h"
#include "storage/lock.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/guc.h"
#include "utils/palloc.h"
#include "utils/rel.h"


/* controls use of locks to enforce safe commutativity */
bool AllModificationsCommutative = false;

/* informs pg_shard to use the CitusDB planner */
bool UseCitusDBSelectLogic = false;


/* local function forward declarations */
static PlannedStmt * PgShardPlannerHook(Query *parse, int cursorOptions,
										ParamListInfo boundParams);
static PlannerType DeterminePlannerType(Query *query);
static Oid ExtractFirstDistributedTableId(Query *query);
static void PgShardExecutorStart(QueryDesc *queryDesc, int eflags);
static void PgShardExecutorRun(QueryDesc *queryDesc, ScanDirection direction, long count);
static bool ExtractRangeTableEntryWalker(Node *node, List **rangeTableList);
static void ErrorIfQueryNotSupported(Query *queryTree);
static List * DistributedQueryShardList(Query *query);
static List * QueryRestrictList(Query *query);
static int32 ExecuteDistributedModify(DistributedPlan *distributedPlan);
static Const * ExtractPartitionValue(Query *query, Var *partitionColumn);
static bool ExtractFromExpressionWalker(Node *node, List **qualifierList);
static Query * RowAndColumnFilterQuery(Query *query);
static List * QueryFromList(List *rangeTableList);
static List * TargetEntryList(List *expressionList);
static DistributedPlan * BuildDistributedPlan(Query *query, List *shardIntervalList);
static void ExecuteDistributedSelect(DistributedPlan *distributedPlan,
									 EState *executorState, DestReceiver *destination,
									 TupleDesc tupleDescriptor);
static bool ExecuteRemoteQuery(char *nodeName, int32 nodePort, StringInfo query,
							   TextRow **rowArray, uint32 *rowCount, uint32 *columnCount);
static bool IsPgShardPlan(PlannedStmt *plannedStmt);
static LOCKMODE CommutativityRuleToLockMode(CmdType commandType);
static void AcquireExecutorShardLocks(List *taskList, LOCKMODE lockMode);
static int CompareTasksByShardId(const void *leftElement, const void *rightElement);
static void LockShard(int64 shardId, LOCKMODE lockMode);


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

	DefineCustomBoolVariable("pg_shard.all_modifications_commutative",
							 "Bypasses commutativity checks when enabled", NULL,
							 &AllModificationsCommutative, false, PGC_USERSET, 0, NULL,
							 NULL, NULL);

	DefineCustomBoolVariable("pg_shard.use_citusdb_select_logic",
							 "Informs pg_shard to use CitusDB's select logic", NULL,
							 &UseCitusDBSelectLogic, false, PGC_USERSET, 0, NULL,
							 NULL, NULL);

	EmitWarningsOnPlaceholders("pg_shard");
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
		List *queryShardList = NIL;

		/* call standard planner first to have Query transformations performed */
		plannedStatement = standard_planner(distributedQuery, cursorOptions,
											boundParams);

		ErrorIfQueryNotSupported(distributedQuery);

		/* compute the list of shards this query needs to access */
		queryShardList = DistributedQueryShardList(distributedQuery);

		/*
		 * If a select query touches multiple shards, we don't push down the
		 * query as-is, and instead only push down the filter clauses and select
		 * needed columns.
		 */
		if ((query->commandType == CMD_SELECT) && (list_length(queryShardList) > 1))
		{
			distributedQuery = RowAndColumnFilterQuery(distributedQuery);
		}

		distributedPlan = BuildDistributedPlan(distributedQuery, queryShardList);
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
	CmdType commandType = query->commandType;

	if (commandType == CMD_SELECT && UseCitusDBSelectLogic)
	{
		plannerType = PLANNER_TYPE_CITUSDB;
	}
	else if (commandType == CMD_SELECT || commandType == CMD_INSERT ||
			 commandType == CMD_UPDATE || commandType == CMD_DELETE)
	{
		Oid distributedTableId = ExtractFirstDistributedTableId(query);
		if (OidIsValid(distributedTableId))
		{
			plannerType = PLANNER_TYPE_PG_SHARD;
		}
		else
		{
			plannerType = PLANNER_TYPE_POSTGRES;
		}
	}
	else
	{
		/*
		 * For utility statements, we need to detect if they are operating on
		 * distributed tables. If they are, we need to warn or error out
		 * accordingly.
		 */
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
		LOCKMODE lockMode = CommutativityRuleToLockMode(plannedStatement->commandType);
		if (lockMode != NoLock)
		{
			AcquireExecutorShardLocks(distributedPlan->taskList, lockMode);
		}

		/* swap back to the distributed plan for rest of query */
		plannedStatement->planTree = (Plan *) distributedPlan;
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
static void
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

		/* we only support default scan direction and row fetch count */
		if (!ScanDirectionIsForward(direction))
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("scan directions other than forward scans "
								   "are unsupported")));
		}
		if (count != 0)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("fetching rows from a query using a cursor "
								   "is unsupported")));
		}

		oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

		if (queryDesc->totaltime != NULL)
		{
			InstrStartNode(queryDesc->totaltime);
		}

		if (operation == CMD_INSERT || operation == CMD_UPDATE ||
			operation == CMD_DELETE)
		{
			int32 affectedRowCount = ExecuteDistributedModify(plan);
			estate->es_processed = affectedRowCount;
		}
		else if (operation == CMD_SELECT)
		{
			DestReceiver *destination = queryDesc->dest;
			List *targetList = plan->targetList;
			TupleDesc tupleDescriptor = ExecCleanTypeFromTL(targetList, false);

			ExecuteDistributedSelect(plan, estate, destination, tupleDescriptor);
		}
		else
		{
			ereport(ERROR, (errmsg("unrecognized operation code: %d",
								   (int) operation)));
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
	Oid distributedTableId = ExtractFirstDistributedTableId(queryTree);
	Var *partitionColumn = PartitionColumn(distributedTableId);
	List *rangeTableList = NIL;
	ListCell *rangeTableCell = NULL;
	bool hasValuesScan = false;
	uint32 queryTableCount = 0;
	bool hasNonConstTargetEntryExprs = false;
	bool specifiesPartitionValue = false;

	CmdType commandType = queryTree->commandType;
	Assert(commandType == CMD_SELECT || commandType == CMD_INSERT ||
		   commandType == CMD_UPDATE || commandType == CMD_DELETE);

	/* prevent utility statements like DECLARE CURSOR attached to selects */
	if (commandType == CMD_SELECT && queryTree->utilityStmt != NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("unsupported utility statement")));
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
						errmsg("cannot plan sharded modification that uses a "
							   "RETURNING clause")));
	}

	if (commandType == CMD_INSERT || commandType == CMD_UPDATE ||
		commandType == CMD_DELETE)
	{
		ListCell *targetEntryCell = NULL;

		foreach(targetEntryCell, queryTree->targetList)
		{
			TargetEntry *targetEntry = (TargetEntry *) lfirst(targetEntryCell);

			/* skip resjunk entries: UPDATE adds some for ctid, etc. */
			if (targetEntry->resjunk)
			{
				continue;
			}

			if (!IsA(targetEntry->expr, Const))
			{
				hasNonConstTargetEntryExprs = true;
			}

			if (targetEntry->resno == partitionColumn->varattno)
			{
				specifiesPartitionValue = true;
			}
		}
	}

	if (hasNonConstTargetEntryExprs)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot plan sharded modification containing values "
							   "which are not constants or constant expressions")));
	}

	if (specifiesPartitionValue && (commandType == CMD_UPDATE))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("modifying the partition value of rows is not allowed")));
	}
}


/*
 * DistributedQueryShardList prunes the shards for the table in the query based
 * on the query's restriction qualifiers, and returns this list.
 */
static List *
DistributedQueryShardList(Query *query)
{
	Oid distributedTableId = ExtractFirstDistributedTableId(query);

	List *restrictClauseList = QueryRestrictList(query);
	List *shardList = LoadShardList(distributedTableId);
	List *prunedShardList = PruneShardList(distributedTableId, restrictClauseList,
										   shardList);

	return prunedShardList;
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
	CmdType commandType = query->commandType;

	if (commandType == CMD_INSERT)
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
	else if (commandType == CMD_SELECT || commandType == CMD_UPDATE ||
			 commandType == CMD_DELETE)
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
						errmsg("cannot modify multiple shards during a single query")));
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
 * RowAndColumnFilterQuery builds a query which contains the filter clauses from
 * the original query and also only selects columns needed for the original
 * query. This new query can then be pushed down to the worker nodes.
 */
static Query *
RowAndColumnFilterQuery(Query *query)
{
	Query *filterQuery = NULL;
	List *rangeTableList = NIL;
	List *whereClauseList = NIL;
	List *whereClauseColumnList = NIL;
	List *projectColumnList = NIL;
	List *columnList = NIL;
	ListCell *columnCell = NULL;
	List *uniqueColumnList = NIL;
	List *targetList = NIL;
	FromExpr *fromExpr = NULL;
	PVCAggregateBehavior aggregateBehavior = PVC_RECURSE_AGGREGATES;
	PVCPlaceHolderBehavior placeHolderBehavior = PVC_REJECT_PLACEHOLDERS;

	ExtractRangeTableEntryWalker((Node *) query, &rangeTableList);
	Assert(list_length(rangeTableList) == 1);

	/* build the where-clause expression */
	whereClauseList = QueryRestrictList(query);
	fromExpr = makeNode(FromExpr);
	fromExpr->quals = (Node *) make_ands_explicit((List *) whereClauseList);
	fromExpr->fromlist = QueryFromList(rangeTableList);

	/* extract columns from both the where and projection clauses */
	whereClauseColumnList = pull_var_clause((Node *) fromExpr, aggregateBehavior,
											placeHolderBehavior);
	projectColumnList = pull_var_clause((Node *) query->targetList, aggregateBehavior,
										placeHolderBehavior);
	columnList = list_union(whereClauseColumnList, projectColumnList);

	/*
	 * list_union() filters duplicates, but only between the lists. For example,
	 * if we have a where clause like (where order_id = 1 OR order_id = 2), we
	 * end up with two order_id columns. We therefore de-dupe the columns here.
	 */
	foreach(columnCell, columnList)
	{
		Var *column = (Var *) lfirst(columnCell);

		uniqueColumnList = list_append_unique(uniqueColumnList, column);
	}

	/*
     * If the query is a simple "SELECT count(*)", add a NULL constant. This
     * constant deparses to "SELECT NULL FROM ...". postgres_fdw generates a
     * similar string when no columns are selected.
     */
	if (uniqueColumnList == NIL)
	{
		/* values for NULL const taken from parse_node.c */
		Const *nullConst = makeConst(UNKNOWNOID, -1, InvalidOid, -2,
									 (Datum) 0, true, false);

		uniqueColumnList = lappend(uniqueColumnList, nullConst);
	}

	targetList = TargetEntryList(uniqueColumnList);

	filterQuery = makeNode(Query);
	filterQuery->commandType = CMD_SELECT;
	filterQuery->rtable = rangeTableList;
	filterQuery->targetList = targetList;
	filterQuery->jointree = fromExpr;

	return filterQuery;
}


/*
 * QueryFromList creates the from list construct that is used for building the
 * query's join tree. The function creates the from list by making a range table
 * reference for each entry in the given range table list.
 */
static List *
QueryFromList(List *rangeTableList)
{
	List *fromList = NIL;
	Index rangeTableIndex = 1;
	uint32 rangeTableCount = (uint32) list_length(rangeTableList);

	for (rangeTableIndex = 1; rangeTableIndex <= rangeTableCount; rangeTableIndex++)
	{
		RangeTblRef *rangeTableReference = makeNode(RangeTblRef);
		rangeTableReference->rtindex = rangeTableIndex;

		fromList = lappend(fromList, rangeTableReference);
	}

	return fromList;
}


/*
 * TargetEntryList creates a target entry for each expression in the given list,
 * and returns the newly created target entries in a list.
 */
static List *
TargetEntryList(List *expressionList)
{
	List *targetEntryList = NIL;
	ListCell *expressionCell = NULL;

	foreach(expressionCell, expressionList)
	{
		Expr *expression = (Expr *) lfirst(expressionCell);

		TargetEntry *targetEntry = makeTargetEntry(expression, -1, NULL, false);
		targetEntryList = lappend(targetEntryList, targetEntry);
	}

	return targetEntryList;
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
	distributedPlan->targetList = query->targetList;

	foreach(shardIntervalCell, shardIntervalList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(shardIntervalCell);
		int64 shardId = shardInterval->id;
		List *finalizedPlacementList = LoadFinalizedShardPlacementList(shardId);
		Task *task = NULL;
		StringInfo queryString = makeStringInfo();

		/* 
		 * Convert the qualifiers to an explicitly and'd clause, which is needed
		 * before we deparse the query. This applies to SELECT, UPDATE and
		 * DELETE statements.
		 */
		FromExpr *joinTree = query->jointree;
		if ((joinTree != NULL) && (joinTree->quals != NULL))
		{
			Node *whereClause = joinTree->quals;
			if (IsA(whereClause, List))
			{
				joinTree->quals = (Node *) make_ands_explicit((List *) whereClause);
			}
		}

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
 * ExecuteDistributedSelect executes the remote select query and sends the
 * resultant tuples to the given destination receiver. If the query fails on a
 * given placement, the function attempts it on its replica.
 */
static void
ExecuteDistributedSelect(DistributedPlan *distributedPlan, EState *executorState,
						 DestReceiver *destination, TupleDesc tupleDescriptor)
{
	uint32 rowIndex = 0;
	uint32 rowCount = 0;
	uint32 columnCount = 0;
	uint32 expectedColumnCount = 0;
	TextRow *rowArray = NULL;
	Task *task = NULL;
	List *taskPlacementList = NIL;
	ListCell *taskPlacementCell = NULL;
	TupleTableSlot *tupleTableSlot = MakeSingleTupleTableSlot(tupleDescriptor);
	AttInMetadata *attributeInputMetadata = NULL;

	List *taskList = distributedPlan->taskList;
	if (list_length(taskList) != 1)
	{
		ereport(ERROR, (errmsg("cannot execute select over multiple shards")));
	}

	task = (Task *) linitial(taskList);
	taskPlacementList = task->taskPlacementList;

	/*
	 * Try to run the query to completion on one placement. If the query fails
	 * attempt the query on the next placement.
	 */
	foreach(taskPlacementCell, taskPlacementList)
	{
		ShardPlacement *taskPlacement = (ShardPlacement *) lfirst(taskPlacementCell);
		bool queryOK = ExecuteRemoteQuery(taskPlacement->nodeName, taskPlacement->nodePort,
										  task->queryString,
										  &rowArray, &rowCount, &columnCount);
		if (queryOK)
		{
			Assert(rowArray != NULL);
			break;
		}
	}

	if (rowArray == NULL)
	{
		ereport(ERROR, (errmsg("unable to execute remote query")));
	}

	expectedColumnCount = tupleDescriptor->natts;
	if (columnCount != expectedColumnCount)
	{
		ereport(ERROR, (errmsg("sql query returned unexpected number of fields")));
	}

	/* initialize the attribute input information */
	attributeInputMetadata = TupleDescGetAttInMetadata(tupleDescriptor);

	/* startup the tuple receiver */
	(*destination->rStartup) (destination, CMD_SELECT, tupleDescriptor);

	/* now iterate over each row and send the tuples to the given destination */
	for (rowIndex = 0; rowIndex < rowCount; rowIndex++)
	{
		TextRow textRow = rowArray[rowIndex];
		HeapTuple heapTuple = BuildTupleFromCStrings(attributeInputMetadata, textRow);

		ExecStoreTuple(heapTuple, tupleTableSlot, InvalidBuffer, false);

		(*destination->receiveSlot) (tupleTableSlot, destination);
		executorState->es_processed++;

		ExecClearTuple(tupleTableSlot);
	}

	/* shutdown the tuple receiver */
	(*destination->rShutdown) (destination);
}


/*
 * ExecuteRemoteQuery takes the given query and attempts to execute it on the
 * given node and port combination. If the query succeeds, the function deep
 * copies the resulting rows into the provided rowArray argument, and also sets
 * the row and column counts. The reason we deep copy the results is because the
 * PGresult object is free'd before we return from the function.
 */
static bool
ExecuteRemoteQuery(char *nodeName, int32 nodePort, StringInfo query,
				   TextRow **rowArray, uint32 *rowCount, uint32 *columnCount)
{
	PGresult *result = NULL;
	uint32 rowIndex = 0;
	uint32 columnIndex = 0;

	PGconn *connection = GetConnection(nodeName, nodePort);
	if (connection == NULL)
	{
		ereport(WARNING, (errmsg("could not connect to \"%s:%u\"",
								 nodeName, nodePort)));
		return false;
	}

	/* now execute the query on the remote node */
	result = PQexec(connection, query->data);
	if (PQresultStatus(result) != PGRES_TUPLES_OK)
	{
		ReportRemoteError(connection, result);
		return false;
	}

	(*rowCount) = PQntuples(result);
	(*columnCount) = PQnfields(result);
	(*rowArray) = (TextRow *) palloc0((*rowCount) * sizeof(TextRow));

	for (rowIndex = 0; rowIndex < (*rowCount); rowIndex++)
	{
		(*rowArray)[rowIndex] = (TextRow) palloc0((*columnCount) * sizeof(char *));
		for (columnIndex = 0; columnIndex < (*columnCount); columnIndex++)
		{
			if (!PQgetisnull(result, rowIndex, columnIndex))
			{
				/* deep copy the value into our result array */
				char *rowValue = PQgetvalue(result, rowIndex, columnIndex);
				(*rowArray)[rowIndex][columnIndex] = pstrdup(rowValue);
			}
			else
			{
				(*rowArray)[rowIndex][columnIndex] = NULL;
			}
		}
	}

	PQclear(result);

	return true;
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
 * CommutativityRuleToLockMode determines the commutativity rule for the given
 * command and returns the appropriate lock mode to enforce that rule. The
 * function assumes a SELECT doesn't modify state and therefore is commutative
 * with all other commands. The function also assumes that an INSERT commutes
 * with another INSERT, but not with an UPDATE/DELETE; and an UPDATE/DELETE
 * doesn't commute with an INSERT, UPDATE, or DELETE.
 *
 * The above mapping is overridden entirely when all_modifications_commutative
 * is set to true. In that case, no commands use any locks whatsoever, meaning
 * all commands may commute with all others.
 */
static LOCKMODE
CommutativityRuleToLockMode(CmdType commandType)
{
	LOCKMODE lockMode = NoLock;

	/* bypass commutativity checks when flag enabled */
	if (AllModificationsCommutative)
	{
		return NoLock;
	}

	if (commandType == CMD_SELECT)
	{
		lockMode = NoLock;
	}
	else if (commandType == CMD_INSERT)
	{
		lockMode = ShareLock;
	}
	else if (commandType == CMD_UPDATE || commandType == CMD_DELETE)
	{
		lockMode = ExclusiveLock;
	}
	else
	{
		ereport(ERROR, (errmsg("unrecognized operation code: %d", (int) commandType)));
	}

	return lockMode;
}


/*
 * AcquireExecutorShardLocks: acquire shard locks needed for execution of tasks
 * within a distributed plan.
 */
static void
AcquireExecutorShardLocks(List *taskList, LOCKMODE lockMode)
{
	List *shardIdSortedTaskList = SortList(taskList, CompareTasksByShardId);
	ListCell *taskCell = NULL;

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


/*
 * LockShard returns after acquiring a lock for the specified shard, blocking
 * indefinitely if required. Only the ExclusiveLock and ShareLock modes are
 * supported: all others will trigger an error. Locks acquired with this method
 * are automatically released at transaction end.
 */
void
LockShard(int64 shardId, LOCKMODE lockMode)
{
	/* locks use 32-bit identifier fields, so split shardId */
	uint32 keyUpperHalf = (uint32) (shardId >> 32);
	uint32 keyLowerHalf = (uint32) shardId;

	LOCKTAG lockTag;
	memset(&lockTag, 0, sizeof(LOCKTAG));

	SET_LOCKTAG_ADVISORY(lockTag, MyDatabaseId, keyUpperHalf, keyLowerHalf, 0);

	if (lockMode == ExclusiveLock || lockMode == ShareLock)
	{
		bool sessionLock = false;	/* we want a transaction lock */
		bool dontWait = false;		/* block indefinitely until acquired */

		(void) LockAcquire(&lockTag, lockMode, sessionLock, dontWait);
	}
	else
	{
		ereport(ERROR, (errmsg("attempted to lock shard using unsupported mode")));
	}
}
