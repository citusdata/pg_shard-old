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

#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "lib/stringinfo.h"


/*
 * DistributedNodeTag identifies nodes used in the planning and execution of
 * queries interacting with distributed tables.
 */
typedef enum DistributedNodeTag
{
	/* Tags for distributed planning begin a safe distance after all other tags. */
	T_DistributedPlan = 2100,		/* plan to be built and passed to executor */
} DistributedNodeTag;


/*
 * PlannerType identifies the type of planner which should be used for a given
 * query.
 */
typedef enum PlannerType
{
	PLANNER_INVALID_FIRST = 0,
	PLANNER_TYPE_CITUSDB = 1,
	PLANNER_TYPE_PG_SHARD = 2,
	PLANNER_TYPE_POSTGRES = 3
} PlannerType;


/*
 * DistributedPlan contains a set of tasks to be executed remotely as part of a
 * distributed query.
 */
typedef struct DistributedPlan
{
	Plan plan;			/* this is a "subclass" of Plan */
	Plan *originalPlan;	/* we save a copy of standard_planner's output */
	List *taskList;		/* list of tasks to run as part of this plan */
} DistributedPlan;


/*
 * Tasks just bundle a query string (already ready for execution) with a list of
 * placements on which that string could be executed. The semantics of a task
 * will vary based on the type of statement being executed: an INSERT must be
 * executed on all placements, but a SELECT might view subsequent placements as
 * fallbacks to be used only if the first placement fails to respond.
 */
typedef struct Task
{
	StringInfo queryString;		/* SQL string suitable for immediate remote execution */
	List *taskPlacementList;	/* ShardPlacements on which the task can be executed */
	int64 shardId;				/* Denormalized shardId of tasks for convenience */
} Task;


/* Function declarations for extension loading and unloading */
extern void _PG_init(void);
extern void _PG_fini(void);


#endif /* PG_SHARD_H */
