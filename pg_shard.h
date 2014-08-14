/*-------------------------------------------------------------------------
 *
 * pg_shard.h
 *			Type and function declarations for pg_shard extension
 *
 * Portions Copyright (c) 2014, Citus Data, Inc.
 *
 * IDENTIFICATION
 *			pg_shard.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_SHARD_H
#define PG_SHARD_H

#include "c.h"
#include "postgres_ext.h"

#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "nodes/primnodes.h"
#include "lib/stringinfo.h"


/*
 * Wrap makeNode to suppress compiler warnings. A DistributedNodeTag will be
 * compared to a NodeTag, which isn't smiled upon by the compiler.
 */
#define makeDistNode(_type_) \
(	_Pragma ("GCC diagnostic push") \
	_Pragma ("GCC diagnostic ignored \"-Wgnu-statement-expression\"") \
	_Pragma ("GCC diagnostic ignored \"-Wenum-conversion\"") \
	makeNode(_type_) \
	_Pragma ("GCC diagnostic pop") \
)

/*
 * DistributedNodeTag identifies nodes used in the planning and execution of
 * queries interacting with distributed tables.
 */
typedef enum DistributedNodeTag
{
	/* Tags for distributed planning begin a safe distance after all other tags. */
	T_DistributedPlan = 2100,		/* plan to be built and passed to executor */
	T_DistributedPlannerInfo = 2500	/* internal state during planning */
} DistributedNodeTag;


/*
 * DistributedPlannerInfo contains the private state the planner builds up when
 * planning a distributed query. Once this structure is complete, it is used to
 * generate a DistributedPlan node to pass to the executor.
 */
typedef struct DistributedPlannerInfo
{
	Query *query;			/* the top-level query being planned */
	Oid distributedTableId;	/* identifies the target table of the distributed plan */
	Var *partitionColumn;	/* stores the column on which the table is partitioned */
	/*
	 * TODO: For SELECT, change the following to use a list of quals rather than
	 *       a list of values, and a list of shardIds, rather than a single one.
	 */
	List *partitionValues;	/* Const values of the partitioned column */
	int64 shardId;			/* the shard affected by the query being planned */
} DistributedPlannerInfo;


/*
 * DistributedPlan contains a set of tasks to be executed remotely as part of a
 * distributed query.
 */
typedef struct DistributedPlan
{
	Plan plan;		/* this is a "subclass" of Plan */
	List *taskList;	/* list of tasks to run as part of this plan */
} DistributedPlan;


/*
 * Tasks just bundle a query string (already ready for execution) with a list of
 * placements on which that string could be executed. The semantics of a task
 * will vary based on the type of statement being executed: an INSERT must be
 * executed on all placements, but a SELECT might view subsequent placements as
 * fallbacks to be used only if the first placement fails to respond.
 */
typedef struct Task {
	StringInfo queryString;		/* SQL string suitable for immediate remote execution */
	List *taskPlacementList;	/* ShardPlacements on which the task can be executed */
} Task;


/* Function declarations for extension loading and unloading */
extern void _PG_init(void);
extern void _PG_fini(void);


#endif /* PG_SHARD_H */
