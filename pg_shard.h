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

#include "postgres_ext.h"

#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
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
 * queries interacting with distributed tables. The values should be assigned
 * from the gaps in NodeTag numbering.
 */
typedef enum DistributedNodeTag
{
	/* Tags for distributed planning begin a safe distance after all other tags. */
	T_DistributedPlan = 2000,
	T_DistributedModifyTable
} DistributedNodeTag;


/*
 * DistributedPlan is the "superclass" of all nodes related to distributed
 * planning and contains fields determined to be relevant to planning of any
 * distributed command type.
 */
typedef struct DistributedPlan
{
	Plan plan;
	Oid relationId;	/* identifies the plan's distributed table */
} DistributedPlan;


/*
 * DistributedModifyTable contains all information needed to modify a particular
 * distributed table. It identifies the type of modification (INSERT, UPDATE, or
 * DELETE), contains an SQL string that can be sent as-is (along with parameter
 * values) to remote nodes, a source plan (specifically a Result or ValuesScan)
 * to produce those values, and a list of placements on which the modification
 * should take place.
 */
typedef struct DistributedModifyTable
{
	DistributedPlan distributedPlan;
	CmdType operation;		/* INSERT, UPDATE, or DELETE */
	StringInfo sql;			/* SQL statement to be executed */
	Plan *sourcePlan;		/* plan producing source data */
	List *shardPlacements;	/* placements on which to execute the statement */
} DistributedModifyTable;


/* Function declarations for extension loading and unloading */
extern void _PG_init(void);
extern void _PG_fini(void);


#endif /* PG_SHARD_H */
