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
#include "postgres_ext.h"


#include "pg_shard.h"
#include "distribution_metadata.h"
#include "partition_pruning.h"
#include "ruleutils.h"

#include <stddef.h>

#include "access/skey.h"
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
#include "utils/palloc.h"


/* local forward declarations */
static PlannedStmt * PgShardPlannerHook(Query *parse, int cursorOptions,
										ParamListInfo boundParams);
static bool NeedsDistributedPlanning(Query *queryTree);
static bool ExtractRangeTableEntryWalker(Node *node, List **rangeTableList);
static DistributedPlan * PlanDistributedModify(Query *query);
static Const * ExtractPartitionValue(Query *query, Var *partitionColumn);
static List * FindTargetShardList(Oid distributedTableId, Var *partitionColumn,
								  Const *partitionValue);
static DistributedPlan * BuildDistributedPlan(Query *query, List *shardIntervalList);
static void UpdateRightOpConst(const OpExpr *clause, Const *constNode);


/* declarations for dynamic loading */
PG_MODULE_MAGIC;


/* saved hook values in case of unload */
static planner_hook_type PreviousPlannerHook = NULL;


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
}


/*
 * _PG_fini is called when the module is unloaded. This function uninstalls the
 * extension's hooks.
 */
void
_PG_fini(void)
{
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

	if (NeedsDistributedPlanning(query))
	{
		Query *distributedQuery = copyObject(query);
		CmdType cmdType = query->commandType;

		/* call standard planner on copy to have Query transformations performed */
		plannedStatement = standard_planner(distributedQuery, cursorOptions,
											boundParams);

		if (cmdType == CMD_INSERT)
		{
			DistributedPlan *distributedPlan = PlanDistributedModify(distributedQuery);

			plannedStatement->planTree = (Plan *) distributedPlan;
		}
	}

	/* if we weren't able to plan the query, use previous hook or standard */
	if (plannedStatement == NULL)
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

	return plannedStatement;
}


/*
 * NeedsDistributedPlanning checks if the passed in Query is an INSERT command
 * running on distributed tables. If it is, we start distributed planning.
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

	if (queryTree->commandType != CMD_INSERT)
	{
		return false;
	}

	/* extract range table entries for simple relations only */
	ExtractRangeTableEntryWalker((Node *) queryTree, &rangeTableList);

	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);

		if (rangeTableEntry->rtekind == RTE_VALUES)
		{
			hasValuesScan = true;
		}
		else if (IsDistributedTable(rangeTableEntry->relid))
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
								   "distributed tables")));
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
 * ExtractRangeTableEntryWalker walks over a query tree, and finds all range
 * table entries that are plain relations or values scans. For recursing into
 * the query tree, this function uses the query tree walker since the expression
 * tree walker doesn't recurse into sub-queries.
 *
 * Values scans are not supported, but there is no field on the Query that can
 * be easily checked to detect them, so we collect them here and let the logic
 * in NeedsDistributedPlanning sort it out.
 */
static bool
ExtractRangeTableEntryWalker(Node *node, List **rangeTableList)
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
		walkerResult = query_tree_walker((Query *) node, ExtractRangeTableEntryWalker,
										 rangeTableList, QTW_EXAMINE_RTES);
	}
	else
	{
		walkerResult = expression_tree_walker(node, ExtractRangeTableEntryWalker,
											  rangeTableList);
	}

	return walkerResult;
}


/*
 * PlanDistributedModify is the main entry point when planning a modification of
 * a distributed table. A DistributedPlan for the query in question is returned,
 * unless the query uses unsupported features, in which case this function will
 * throw an error.
 */
static DistributedPlan *
PlanDistributedModify(Query *query)
{
	DistributedPlan *distributedPlan = NULL;
	Oid resultTableId = getrelid(query->resultRelation, query->rtable);
	Var *partitionColumn = PartitionColumn(resultTableId);
	Const *partitionValue = NULL;
	List *targetShardList = NIL;

	/* reject queries with a returning list */
	if (list_length(query->returningList) > 0)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot plan INSERT to a distributed table that "
								"uses a RETURNING clause")));
	}

	/* use values of partition columns to determine shards, then build plan */
	partitionValue = ExtractPartitionValue(query, partitionColumn);
	targetShardList = FindTargetShardList(resultTableId, partitionColumn,
										  partitionValue);
	distributedPlan = BuildDistributedPlan(query, targetShardList);

	return distributedPlan;
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
 * FindTargetShardInterval locates shards capable of receiving rows with the
 * provided partition value. If no such shards exist, this method returns NIL.
 */
static List *
FindTargetShardList(Oid distributedTableId, Var *partitionColumn, Const *partitionValue)
{
	List *shardList = LoadShardList(distributedTableId);
	List *whereClauseList = NIL;
	List *targetShardList = NIL;

	/* build equality expression based on partition column value for row */
	OpExpr *equalityExpr = MakeOpExpression(partitionColumn, BTEqualStrategyNumber);
	UpdateRightOpConst(equalityExpr, partitionValue);
	whereClauseList = list_make1(equalityExpr);

	targetShardList = PruneShardList(distributedTableId, whereClauseList, shardList);

	return targetShardList;
}


/*
 * BuildDistributedPlan simply creates the DistributedPlan instance from the
 * provided query and shard interval list.
 */
static DistributedPlan *
BuildDistributedPlan(Query *query, List *shardIntervalList)
{
	DistributedPlan *distributedPlan = makeDistNode(DistributedPlan);
	ListCell *shardIntervalCell = NULL;
	List *taskList = NIL;

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

	return distributedPlan;
}


/*
 * UpdateRightOpValue updates the provided clause (in-place) by replacing its
 * right-hand side with the provided value.
 */
static void
UpdateRightOpConst(const OpExpr *clause, Const *constNode)
{
	Node *rightOp = get_rightop((Expr *) clause);
	Const *rightConst = NULL;

	Assert(IsA(rightOp, Const));

	rightConst = (Const *) rightOp;

	rightConst->constvalue = constNode->constvalue;
	rightConst->constisnull = constNode->constisnull;
	rightConst->constbyval = constNode->constbyval;
}
