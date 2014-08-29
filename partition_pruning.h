/*-------------------------------------------------------------------------
 *
 * partition_pruning.h
 *			Pruning function declarations for pg_shard extension
 *
 * Portions Copyright (c) 2014, Citus Data, Inc.
 *
 * IDENTIFICATION
 *			partition_pruning.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_SHARD_PARTITION_PRUNING_H
#define PG_SHARD_PARTITION_PRUNING_H

#include "access/attnum.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"


#define DISTRIBUTE_BY_HASH		'h'
#define RESERVED_HASHED_COLUMN_ID	MaxAttrNumber


extern List * PruneShardList(Oid relationId, List *whereClauseList, List *shardList);
extern OpExpr * MakeOpExpression(Var *variable, int16 strategyNumber);


#endif /* PG_SHARD_PARTITION_PRUNING_H */