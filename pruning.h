/*-------------------------------------------------------------------------
 *
 * pruning.h
 *			Pruning function declarations for pg_shard extension
 *
 * Portions Copyright (c) 2014, Citus Data, Inc.
 *
 * IDENTIFICATION
 *			pruning.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_SHARD_PRUNING_H
#define PG_SHARD_PRUNING_H

#include "access/attnum.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"

#define DISTRIBUTE_BY_HASH		'h'
#define HASHED_COLUMN_NUMBER	MaxAttrNumber

extern List * PruneShardList(Var *partitionColumn, List *whereClauseList,
							 List *shardList);

#endif /* PG_SHARD_PRUNING_H */
