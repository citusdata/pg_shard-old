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
#include "pg_config.h"

#include "pg_shard.h"
#include "distribution_metadata.h"
#include "ruleutils.h"

#include <stddef.h>

#include "access/skey.h"
#include "catalog/pg_am.h"
#include "commands/defrem.h"
#include "nodes/makefuncs.h"
#include "nodes/nodes.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "nodes/primnodes.h"
#include "nodes/relation.h"
#include "optimizer/clauses.h"
#include "optimizer/planner.h"
#include "optimizer/predtest.h"
#include "optimizer/restrictinfo.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/lsyscache.h"
#include "utils/palloc.h"
#include "utils/typcache.h"


/* local forward declarations */
static PlannedStmt * PgShardPlannerHook(Query *parse, int cursorOptions,
										ParamListInfo boundParams);
static DistributedModifyTable * PlanDistributedInsert(Query *query,
													  ModifyTable *ModifyTable);
static void VerifyInsertIsLegal(Query *query, Oid distributedTableId);
static DistributedModifyTable * TransformModifyTable(ModifyTable *modifyTable,
													 Query *query, Oid distributedTableId,
													 Var *partitionColumn);
static List * ExtractPartitionValues(Plan *sourcePlan, Var *partitionColumn);
static ShardInterval * FindTargetShardInterval(List *shardList, List *partitionValues,
											   Var *partitionColumn);
static List * BuildPartitionValueRestrictInfoList(List *partitionValues,
												  Var *partitionColumn);
static List * PruneShardList(List *restrictInfoList, List *shardList,
							 Var *partitionColumn);
static FunctionCallInfo GetHashFunctionByType(Oid typeId);
static Node * BuildBaseConstraint(Var *partitionColumn);
static void UpdateConstraint(Node *baseConstraint, ShardInterval *shardInterval);
static OpExpr * MakeOpExpression(Var *variable, int16 strategyNumber);
static Oid GetOperatorByType(Oid typeId, Oid accessMethodId, int16 strategyNumber);
static void UpdateRightOpValue(const OpExpr *clause, Datum value);


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

	switch (query->commandType)
	{
		case CMD_INSERT:
		{
			PlanDistributedInsert(query, (ModifyTable *) plannedStatement->planTree);
			break;
		}
		default:
		{
			break;
		}
	}

	return plannedStatement;
}


/*
 * PlanDistributedInsert is the main entry point when planning an INSERT to a
 * distributed table. It checks whether the INSERT's target table is distributed
 * and produces a DistributedModifyTable node if so. Otherwise, this function
 * returns NULL to avoid further distributed processing.
 */
static DistributedModifyTable *
PlanDistributedInsert(Query *query, ModifyTable *modifyTable)
{
	DistributedModifyTable *distributedModifyTable = NULL;
	RangeTblEntry *resultRangeTableEntry = rt_fetch(query->resultRelation,
													query->rtable);

	if (TableIsDistributed(resultRangeTableEntry->relid))
	{
		Var *partitionColumn = PartitionColumn(resultRangeTableEntry->relid);

		VerifyInsertIsLegal(query, resultRangeTableEntry->relid);

		distributedModifyTable = TransformModifyTable(modifyTable, query,
													  resultRangeTableEntry->relid,
													  partitionColumn);
	}

	return distributedModifyTable;
}


/*
 * VerifyInsertIsLegal inspects a provided query in order to verify that it does
 * not violate any restrictions on the types of INSERT commands allowed by this
 * extension. In particular, only one distributed table may be referenced, and
 * it may not be mixed with references to other local tables. Finally, function
 * scans, subqueries, CTEs, and RETURNING clauses must not appear.
 */
static void
VerifyInsertIsLegal(Query *query, Oid distributedTableId)
{
	ListCell *rangeTableEntryCell = NULL;

	foreach(rangeTableEntryCell, query->rtable)
	{
		RangeTblEntry *rangeTableEntry = lfirst(rangeTableEntryCell);

		if ((rangeTableEntry->relid != distributedTableId) &&
			(rangeTableEntry->rtekind != RTE_VALUES))
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("sharded INSERTs may only reference "
								   "a single table and cannot use sub"
								   "queries, common table expressions, "
								   "or function scans")));
		}
	}

	if (list_length(query->returningList) > 0)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot plan sharded INSERT that uses a "
							   "RETURNING clause")));
	}
}


/*
 * TransformModifyTable accepts a standard ModifyTable plan node and produces a
 * corresponding DistributedModifyTable node populated with information needed
 * by the executor to perform an INSERT to a distributed table.
 */
static DistributedModifyTable *
TransformModifyTable(ModifyTable *modifyTable, Query *query,
					 Oid distributedTableId, Var *partitionColumn)
{
	DistributedModifyTable *distributedModifyTable = NULL;
	Plan *sourcePlan = NULL;
	List *partitionValues = NIL;
	List *shardList = LoadShardList(distributedTableId);
	ShardInterval *targetShardInterval = NULL;
	List *shardPlacements = NIL;

	distributedModifyTable = makeDistNode(DistributedModifyTable);

	/* Fill plan with dummy values for now. */
	distributedModifyTable->distributedPlan.plan.startup_cost = 0;
	distributedModifyTable->distributedPlan.plan.total_cost = 0;
	distributedModifyTable->distributedPlan.plan.plan_rows = 0;
	distributedModifyTable->distributedPlan.plan.plan_width = 0;

	/* Copy over Oid of distributed table */
	distributedModifyTable->distributedPlan.relationId = distributedTableId;

	distributedModifyTable->operation = CMD_INSERT;

	Assert(list_length(modifyTable->plans) == 1);
	sourcePlan = linitial(modifyTable->plans);

	Assert(IsA(sourcePlan, Result) || IsA(sourcePlan, ValuesScan));
	distributedModifyTable->sourcePlan = sourcePlan;

	partitionValues = ExtractPartitionValues(distributedModifyTable->sourcePlan,
											 partitionColumn);
	targetShardInterval = FindTargetShardInterval(shardList, partitionValues,
												  partitionColumn);

	shardPlacements = LoadShardPlacementList(targetShardInterval->id);
	distributedModifyTable->shardPlacements = shardPlacements;

	distributedModifyTable->sql = makeStringInfo();
	distributedModifyTable->sql->data = deparse_shard_query(query,
															targetShardInterval->id);

	ereport(INFO, (errmsg("Distributed SQL: %s", distributedModifyTable->sql->data)));

	return distributedModifyTable;
}


/*
 * ExtractPartitionValues extracts partition column values from the source plan
 * field of a modify table node. The subplan must be either a ValuesScan node or
 * a Result. Returns a list of expressions.
 */
static List *
ExtractPartitionValues(Plan *sourcePlan, Var *partitionColumn)
{
	List *partitionColumnValues = NIL;
	TargetEntry *targetEntry = get_tle_by_resno(sourcePlan->targetlist,
												partitionColumn->varattno);

	switch(nodeTag(sourcePlan))
	{
		case T_Result:
		{
			partitionColumnValues = lappend(partitionColumnValues, targetEntry->expr);
			break;
		}
		case T_ValuesScan:
		{
			ValuesScan *valuesScan = (ValuesScan *) sourcePlan;
			Var *valuesScanPartitionColumn = NULL;
			int valuesScanPartColIndex = 0;
			ListCell *columnValuesCell = NULL;

			Assert(IsA(targetEntry->expr, Var));
			valuesScanPartitionColumn = (Var *) targetEntry->expr;
			valuesScanPartColIndex = valuesScanPartitionColumn->varattno - 1;

			foreach(columnValuesCell, valuesScan->values_lists)
			{
				List *columnValues = (List *) lfirst(columnValuesCell);
				Expr *partitionColumnValue = (Expr *) list_nth(columnValues,
															   valuesScanPartColIndex);

				partitionColumnValues = lappend(partitionColumnValues,
												partitionColumnValue);
			}

			break;
		}
		default:
		{
			ereport(ERROR, (errmsg("source plan of distributed INSERT must be "
								   "a Result or ValuesScan node")));
			break;
		}
	}

	return partitionColumnValues;
}


/*
 * FindTargetShardInterval locates a single shard capable of receiving rows with
 * the specified partition values. If no such shard exists (or if more than one
 * does), this method throws an error.
 */
static ShardInterval *
FindTargetShardInterval(List *shardList, List *partitionValues, Var *partitionColumn)
{
	List *restrictInfoList = BuildPartitionValueRestrictInfoList(partitionValues,
																 partitionColumn);
	List *shardIntervals = PruneShardList(restrictInfoList, shardList, partitionColumn);

	if (list_length(shardIntervals) == 0)
	{
		ereport(ERROR, (errmsg("no shard exists to accept these rows")));
	}
	else if (list_length(shardIntervals) > 1)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot plan query that inserts into more than "
							   "one shard at a time")));
	}

	return (ShardInterval *) linitial(shardIntervals);
}


/*
 * BuildPartitionValueRestrictInfoList builds a single-element list containing a
 * RestrictInfo node whose clause is a disjunction of equality clauses between
 * the provided partition column and hashes of the partition values. This is to
 * express the idea that the hash of a given row's partition value can take on
 * any one of a finite set of values.
 *
 * The provided partition values must be Const nodes: if it was impossible to
 * reduce the partition column value for a particular row to a constant, then
 * the distributed INSERT will fail.
 *
 * NULL values always hash to zero.
 */
static List *
BuildPartitionValueRestrictInfoList(List *partitionValues, Var *partitionColumn)
{
	FunctionCallInfo hashCallInfo = GetHashFunctionByType(partitionColumn->vartype);

	List *hashEqualityClauseList = NIL;
	ListCell *columnValueCell = NULL;
	Expr *orClause = NULL;
	RestrictInfo *hashEqualityRestrictInfo = NULL;

	foreach(columnValueCell, partitionValues)
	{
		Expr *columnValue = (Expr *) lfirst(columnValueCell);
		Const *columnConst = NULL;
		OpExpr *hashEqualityExpr = MakeOpExpression(partitionColumn,
													BTEqualStrategyNumber);
		/* use zero as the default value if column val is NULL */
		Datum hashValue = Int64GetDatum(0);

		if (!IsA(columnValue, Const))
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot plan sharded INSERT using a non-"
								   "constant partition column value")));
		}

		columnConst = (Const *) columnValue;
		if (!columnConst->constisnull)
		{
			hashCallInfo->arg[0] = columnConst->constvalue;

			hashValue = FunctionCallInvoke(hashCallInfo);
		}

		UpdateRightOpValue(hashEqualityExpr, hashValue);

		hashEqualityClauseList = lappend(hashEqualityClauseList, hashEqualityExpr);
	}

	orClause = make_orclause(hashEqualityClauseList);
	hashEqualityRestrictInfo = make_simple_restrictinfo(orClause);

	return list_make1(hashEqualityRestrictInfo);
}


/*
 * PruneShardList takes a list of RestrictInfo nodes, a ShardInterval list, and
 * a reference to the partition column for the table in question. It uses the
 * RestrictInfo nodes to reject elements of the shard list and returns the list
 * after such filtering has been applied.
 */
static List *
PruneShardList(List *restrictInfoList, List *shardList, Var *partitionColumn)
{
	List *remainingShardList = NIL;
	ListCell *shardCell = NULL;

	/* build the base expression for constraint */
	Node *baseConstraint = BuildBaseConstraint(partitionColumn);

	/* walk over shard list and check if shards can be pruned */
	foreach(shardCell, shardList)
	{
		uint64 *shardIdPointer = (uint64 *) lfirst(shardCell);
		uint64 shardId = (*shardIdPointer);
		List *constraintList = NIL;
		bool shardPruned = false;

		ShardInterval *shardInterval = LoadShardInterval(shardId);

		/* set the min/max values in the base constraint */
		UpdateConstraint(baseConstraint, shardInterval);
		constraintList = list_make1(baseConstraint);

		shardPruned = predicate_refuted_by(constraintList, restrictInfoList);
		if (shardPruned)
		{
			ereport(DEBUG2, (errmsg("predicate pruning for shardId "
									UINT64_FORMAT, shardId)));
		}
		else
		{
			remainingShardList = lappend(remainingShardList, shardIdPointer);
		}
	}

	return remainingShardList;
}


/*
 * GetHashFunctionByType locates a default hash function for a type using an Oid
 * for that type. This function raises an error if no such function exists.
 *
 * We assume all hash functions are unary.
 */
static FunctionCallInfo
GetHashFunctionByType(Oid typeId)
{
	FunctionCallInfo fcinfo = (FunctionCallInfo) palloc0(sizeof(FunctionCallInfoData));
	TypeCacheEntry *typeEntry = lookup_type_cache(typeId, TYPECACHE_HASH_PROC_FINFO);

	if (!OidIsValid(typeEntry->hash_proc_finfo.fn_oid))
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
						errmsg("could not identify a hash function for type %s",
							   format_type_be(typeId))));
	}

	InitFunctionCallInfoData(*fcinfo, &typeEntry->hash_proc_finfo, 1,
							 InvalidOid, NULL, NULL);
	fcinfo->arg[0] = 0;
	fcinfo->argnull[0] = false;
	fcinfo->isnull = false;

	return fcinfo;
}


/*
 * BuildBaseConstraint returns an AND clause suitable to test containment of a
 * particular value within an interval. More specifically, for a variable X, the
 * returned clause is ((X >= min) AND (X <= max)). The variables min and max are
 * unbound values to be supplied later (probably retrieved from a distributed
 * table's shard intervals).
 */
static Node *
BuildBaseConstraint(Var *partitionColumn)
{
	Node *baseConstraint = NULL;
	OpExpr *lessThanExpr = NULL;
	OpExpr *greaterThanExpr = NULL;

	/* Build these expressions with only one argument for now */
	lessThanExpr = MakeOpExpression(partitionColumn, BTLessEqualStrategyNumber);
	greaterThanExpr = MakeOpExpression(partitionColumn, BTGreaterEqualStrategyNumber);

	/* Build base constaint as an and of two qual conditions */
	baseConstraint = make_and_qual((Node *) lessThanExpr, (Node *) greaterThanExpr);

	return baseConstraint;
}


/*
 * UpdateConstraint accepts a constraint previously produced by a call to
 * BuildBaseConstraint and updates this constraint with the minimum and maximum
 * values from the provided shard interval.
 */
static void
UpdateConstraint(Node *baseConstraint, ShardInterval *shardInterval)
{
	BoolExpr *andExpr = (BoolExpr *) baseConstraint;
	Node *lessThanExpr = (Node *) linitial(andExpr->args);
	Node *greaterThanExpr = (Node *) lsecond(andExpr->args);

	UpdateRightOpValue((OpExpr *)greaterThanExpr, shardInterval->minValue);
	UpdateRightOpValue((OpExpr *)lessThanExpr, shardInterval->maxValue);
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

	Oid accessMethodId = BTREE_AM_OID;
	Oid operatorId = InvalidOid;
	Const  *constantValue = NULL;
	OpExpr *expression = NULL;

	/* Load the operator from system catalogs */
	operatorId = GetOperatorByType(typeId, accessMethodId, strategyNumber);

	constantValue = makeNullConst(typeId, typeModId, collationId);

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
UpdateRightOpValue(const OpExpr *clause, Datum value)
{
	Node *rightOp = get_rightop((Expr *)clause);
	Const *rightConst = NULL;

	Assert(IsA(rightOp, Const));

	rightConst = (Const *) rightOp;

	rightConst->constvalue = value;
	rightConst->constisnull = false;
	rightConst->constbyval = true;
}
