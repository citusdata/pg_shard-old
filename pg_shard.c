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
#include "pruning.h"
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
static List * BuildPartitionValueWhereList(DistributedPlannerInfo *distRoot);
static OpExpr * MakeOpExpression(Var *variable, int16 strategyNumber);
static Oid GetOperatorByType(Oid typeId, Oid accessMethodId, int16 strategyNumber);
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
 * ExtractPartitionValue extracts partition column values from a list of source
 * plans. For now, exactly one source plan is expected and must be a Result.
 * Once a list of partition column values has been computed, this function saves
 * it in the provided DistributedPlannerInfo.
 */
static void
ExtractPartitionValue(DistributedPlannerInfo *distRoot)
{
	Var *partitionColumn = distRoot->partitionColumn;
	TargetEntry *targetEntry = get_tle_by_resno(distRoot->query->targetList,
												distRoot->partitionColumn->varattno);
	Const *value = makeNullConst(partitionColumn->vartype, partitionColumn->vartypmod,
							   partitionColumn->varcollid);

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
	List *whereClauseList = BuildPartitionValueWhereList(distRoot);
	List *shardIntervalList = PruneShardList(distRoot->partitionColumn, whereClauseList,
											 shardList);
	int shardIntervalCount = list_length(shardIntervalList);
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
 * BuildPartitionValueWhereList builds a single-element list with an equality
 * clause on the partition column of the distributed table being planned. The
 * right-hand side of this clause is set to the particular value of the row
 * being inserted in order to leverage shard pruning logic during INSERT.
 */
static List *
BuildPartitionValueWhereList(DistributedPlannerInfo *distRoot)
{
	Const *columnConst= distRoot->partitionValue;
	OpExpr *equalityExpr = MakeOpExpression(distRoot->partitionColumn,
											BTEqualStrategyNumber);
	UpdateRightOpConst(equalityExpr, columnConst);

	return list_make1(equalityExpr);
}


/*
 * MakeOpExpression simply returns a binary operator expression for the provided
 * variable type and strategy (equal, greater than, less than, etc.).
 */
static OpExpr *
MakeOpExpression(Var *variable, int16 strategyNumber)
{
	Oid typeId = variable->vartype;
	Oid typeModId = variable->vartypmod;
	Oid collationId = variable->varcollid;

	/* Load the operator from system catalogs */
	Oid accessMethodId = BTREE_AM_OID;
	Oid operatorId = GetOperatorByType(typeId, accessMethodId, strategyNumber);

	Const  *constantValue = makeNullConst(typeId, typeModId, collationId);
	OpExpr *expression = NULL;

	/* Now make the expression with the given variable and a null constant */
	expression = (OpExpr *) make_opclause(operatorId,
										  InvalidOid, /* no result type yet */
										  false,	  /* no return set */
										  (Expr *) variable,
										  (Expr *) constantValue,
										  InvalidOid, collationId);

	/* Set implementing function id and result type */
	expression->opfuncid = get_opcode(operatorId);
	expression->opresulttype = get_func_rettype(expression->opfuncid);

	return expression;
}


/*
 * GetOperatorByType returns the identifier of the operator implementing the
 * provided strategy (equal, greater than, etc.) for the provided type using
 * the provided access method (BTree, etc.).
 */
static Oid
GetOperatorByType(Oid typeId, Oid accessMethodId, int16 strategyNumber)
{
	/* Get default operator class from pg_opclass */
	Oid operatorClassId = GetDefaultOpClass(typeId, accessMethodId);

	Oid operatorFamily = get_opclass_family(operatorClassId);

	Oid operatorId = get_opfamily_member(operatorFamily, typeId, typeId, strategyNumber);

	return operatorId;
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
 */
static bool
NeedsDistributedPlanning(Query *queryTree)
{
	List *rangeTableList = NIL;
	ListCell *rangeTableCell = NULL;
	bool hasLocalRelation = false;
	bool hasDistributedRelation = false;

	if (queryTree->commandType != CMD_INSERT)
	{
		return false;
	}

	/* extract range table entries for simple relations only */
	ExtractRangeTableRelationWalker((Node *) queryTree, &rangeTableList);

	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);
		if (TableIsDistributed(rangeTableEntry->relid))
		{
			hasDistributedRelation = true;
		}
		else
		{
			hasLocalRelation = true;
		}
	}

	/* users can't mix local and distributed relations in one query */
	if (hasLocalRelation && hasDistributedRelation)
	{
		ereport(ERROR, (errmsg("cannot plan queries that include both regular and "
							   "partitioned relations")));
	}

	return hasDistributedRelation;
}


/*
 * ExtractRangeTableRelationWalker walks over a query tree, and finds all range
 * table entries that are plain relations. For recursing into the query tree,
 * this function uses the query tree walker since the expression tree walker
 * doesn't recurse into sub-queries.
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
		if (rangeTable->rtekind == RTE_RELATION)
		{
			(*rangeTableList) = lappend(*rangeTableList, rangeTable);
		}
		/* TODO: This check must live elsewhere
		 *	else if (rangeTable->rtekind == RTE_VALUES)
		 *	{
		 *		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		 *						errmsg("multi-row INSERT not supported")));
		 *	}
		 *	else if (rangeTable->rtekind == RTE_CTE)
		 *	{
		 *		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		 *						errmsg("common table expressions not supported")));
		 *	}
		 */
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
