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

#include "pg_shard.h"
#include "distribution_metadata.h"
#include "partition_pruning.h"
#include "ruleutils.h"

#include <stddef.h>

#include "access/skey.h"
#include "catalog/pg_am.h"
#include "commands/defrem.h"
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
#include "parser/parsetree.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/lsyscache.h"
#include "utils/palloc.h"


/* local forward declarations */
static PlannedStmt * PgShardPlannerHook(Query *parse, int cursorOptions,
										ParamListInfo boundParams);
static DistributedPlan * PlanDistributedModify(Query *query);
static Const * ExtractPartitionValue(Query *query, Var *partitionColumn);
static List * FindTargetShardList(Oid distributedTableId, Var *partitionColumn,
								  Const *partitionValue);
static DistributedPlan * BuildDistributedPlan(Query *query, List *targetShardList);
static void UpdateRightOpConst(const OpExpr *clause, Const *constNode);
static bool NeedsDistributedPlanning(Query *queryTree);
static bool ExtractRangeTableRelationWalker(Node *node, List **rangeTableList);


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

	/* call standard planner to have Query transformations performed */
	/* TODO: Where do we call PreviousPlannerHook? */
	plannedStatement = standard_planner(query, cursorOptions, boundParams);

	if (NeedsDistributedPlanning(query))
	{
		CmdType cmdType = query->commandType;

		if (cmdType == CMD_INSERT)
		{
			Task *task = NULL;
			DistributedPlan *distributedPlan = PlanDistributedModify(query);
			task = linitial(distributedPlan->taskList);
			ereport(INFO, (errmsg("%s", task->queryString->data)));
		}

		/* TODO: Add SELECT handling here */
	}

	return plannedStatement;
}


/*
 * PlanDistributedModify is the main entry point when planning a modification of
 * a distributed table. It checks whether the modification's target table is
 * distributed and produces a DistributedPlan node if so. Otherwise, this
 * function returns NULL to avoid further distributed processing.
 */
static DistributedPlan *
PlanDistributedModify(Query *query)
{
	DistributedPlan *distributedPlan = NULL;
	Oid resultTableId = getrelid(query->resultRelation, query->rtable);
	Var *partitionColumn = PartitionColumn(resultTableId);
	Const *partitionValue = NULL;
	List *targetShardList = NIL;

	/* Reject queries with a returning list */
	if (list_length(query->returningList) > 0)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot plan sharded INSERT that uses a "
							   "RETURNING clause")));
	}

	/* use values of partition columns to determine shard */
	partitionValue = ExtractPartitionValue(query, partitionColumn);
	targetShardList = FindTargetShardList(resultTableId, partitionColumn,
										  partitionValue);

	/* use accumulated planner state to generate plan */
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
 * FindTargetShardInterval locates shards capable of receiving rows with the
 * specified partition value. If no such shards exist, this method will return
 * NIL.
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
 * BuildDistributedPlan constructs a distributed plan from a query and list of
 * ShardInterval instances.
 */
static DistributedPlan *
BuildDistributedPlan(Query *query, List *targetShardList)
{
	DistributedPlan *distributedPlan = makeDistNode(DistributedPlan);
	ListCell *targetShardCell = NULL;
	List *taskList = NIL;

	foreach(targetShardCell, targetShardList)
	{
		ShardInterval *targetShard = (ShardInterval *) lfirst(targetShardCell);
		List *shardPlacementList = LoadShardPlacementList(targetShard->id);
		Task *task = NULL;

		StringInfo queryString = makeStringInfo();
		deparse_shard_query(query, targetShard->id, queryString);

		task = (Task *) palloc0(sizeof(Task));
		task->queryString = queryString;
		task->taskPlacementList = shardPlacementList;

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
	Node *rightOp = get_rightop((Expr *)clause);
	Const *rightConst = NULL;

	Assert(IsA(rightOp, Const));

	rightConst = (Const *) rightOp;

	rightConst->constvalue = constNode->constvalue;
	rightConst->constisnull = constNode->constisnull;
	rightConst->constbyval = constNode->constbyval;
}


/*
 * NeedsDistributedPlanning checks if the passed in Query is an INSERT command
 * running on partitioned relations. If it is, we start distributed planning.
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
