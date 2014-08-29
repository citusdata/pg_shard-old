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
static DistributedPlannerInfo * BuildDistributedPlannerInfo(Query *query,
															Oid distributedTableId);
static void ExtractPartitionValue(DistributedPlannerInfo *distRoot);
static void FindTargetShardInterval(DistributedPlannerInfo *distRoot);
static DistributedPlan * BuildDistributedPlan(DistributedPlannerInfo *distRoot);
static void UpdateRightOpConst(const OpExpr *clause, Const *constNode);
static bool NeedsDistributedPlanning(Query *queryTree);
static bool ExtractRangeTableRelationWalker(Node *node, List **rangeTableList);


/* declarations for dynamic loading */
PG_MODULE_MAGIC;


/* Saved hook values in case of unload */
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
		DistributedPlan *distributedPlan = NULL;
		CmdType cmdType = query->commandType;

		if (cmdType == CMD_INSERT)
		{
			Task *task = NULL;

			distributedPlan = PlanDistributedModify(query);
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
	DistributedPlannerInfo *distRoot = NULL;

	/* Reject queries with a returning list */
	if (list_length(query->returningList) > 0)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot plan sharded INSERT that uses a "
							   "RETURNING clause")));
	}

	distRoot = BuildDistributedPlannerInfo(query, resultTableId);

	/* use values of partition columns to determine shard */
	ExtractPartitionValue(distRoot);
	FindTargetShardInterval(distRoot);

	/* use accumulated planner state to generate plan */
	distributedPlan = BuildDistributedPlan(distRoot);

	return distributedPlan;
}


/*
 * BuildDistributedPlannerInfo creates an object to encapsulate common state
 * used by the planner during distributed query planning. Prepopulates the state
 * with the query object, Oid of the distributed table, and partition column.
 */
static DistributedPlannerInfo *
BuildDistributedPlannerInfo(Query *query, Oid distributedTableId)
{
	DistributedPlannerInfo *distRoot = NULL;

	distRoot = makeDistNode(DistributedPlannerInfo);

	distRoot->query = query;
	distRoot->distributedTableId = distributedTableId;
	distRoot->partitionColumn = PartitionColumn(distributedTableId);

	return distRoot;
}


/*
 * ExtractPartitionValue extracts the partition column value from a the target
 * of a modification command. If a partition value is not a constant, is NULL,
 * or is missing altogether, this function throws an error.
 */
static void
ExtractPartitionValue(DistributedPlannerInfo *distRoot)
{
	Const *value = NULL;

	TargetEntry *targetEntry = get_tle_by_resno(distRoot->query->targetList,
												distRoot->partitionColumn->varattno);
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

	distRoot->partitionValue = value;
}


/*
 * FindTargetShardInterval locates a single shard capable of receiving rows with
 * the specified partition values and saves its id in the provided distributed
 * planner info instance. If no such shard exists (or if more than one does),
 * this method throws an error.
 */
static void
FindTargetShardInterval(DistributedPlannerInfo *distRoot)
{
	ShardInterval *shardInterval = NULL;
	List *shardList = LoadShardList(distRoot->distributedTableId);
	List *whereClauseList = NIL;
	List *shardIntervalList = NIL;
	int shardIntervalCount = 0;

	/* build equality expression based on partition column value for row */
	OpExpr *equalityExpr = MakeOpExpression(distRoot->partitionColumn,
											BTEqualStrategyNumber);
	UpdateRightOpConst(equalityExpr, distRoot->partitionValue);
	whereClauseList = list_make1(equalityExpr);

	shardIntervalList = PruneShardList(distRoot->partitionColumn, whereClauseList,
									   shardList);
	shardIntervalCount = list_length(shardIntervalList);
	if (shardIntervalCount == 0)
	{
		ereport(ERROR, (errmsg("no shard exists to accept these rows")));
	}
	else if (shardIntervalCount > 1)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot plan query that inserts into more than "
							   "one shard at a time")));
	}

	shardInterval = (ShardInterval *) linitial(shardIntervalList);
	distRoot->shardId = shardInterval->id;
}


/*
 * BuildDistributedPlan constructs a distributed plan using information which
 * has been collected in a DistributedPlannerInfo instance.
 */
static DistributedPlan *
BuildDistributedPlan(DistributedPlannerInfo *distRoot)
{
	DistributedPlan *distributedPlan = makeDistNode(DistributedPlan);
	List *shardPlacementList = LoadShardPlacementList(distRoot->shardId);
	Task *task = NULL;

	StringInfo queryString = makeStringInfo();
	deparse_shard_query(distRoot->query, distRoot->shardId, queryString);

	task = (Task *) palloc0(sizeof(Task));
	task->queryString = queryString;
	task->taskPlacementList = shardPlacementList;

	distributedPlan->taskList = list_make1(task);

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
