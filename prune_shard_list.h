/*-------------------------------------------------------------------------
 *
 * prune_shard_list.h
 *			Pruning function declarations for pg_shard extension
 *
 * Portions Copyright (c) 2014, Citus Data, Inc.
 *
 * IDENTIFICATION
 *			prune_shard_list.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_SHARD_PRUNE_SHARD_LIST_H
#define PG_SHARD_PRUNE_SHARD_LIST_H

#include "c.h"
#include "postgres_ext.h"

#include "access/attnum.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"


#define DISTRIBUTE_BY_HASH			'h'
#define RESERVED_HASHED_COLUMN_ID	MaxAttrNumber


extern List * PruneShardList(Oid relationId, List *whereClauseList,
							 List *shardIntervalList);
extern OpExpr * MakeOpExpression(Var *variable, int16 strategyNumber);


#endif /* PG_SHARD_PRUNE_SHARD_LIST_H */
