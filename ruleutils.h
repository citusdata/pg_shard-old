/*-------------------------------------------------------------------------
 *
 * ruleutils.h
 *			Function declarations for deparsing queries.
 *
 * Portions Copyright (c) 2014, Citus Data, Inc.
 *
 * IDENTIFICATION
 *			ruleutils.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_SHARD_RULEUTILS_H
#define PG_SHARD_RULEUTILS_H

#include "c.h"

#include "nodes/parsenodes.h"


extern char * deparse_shard_query(Query *query, int64 shardid);

#endif /* PG_SHARD_RULEUTILS_H */
