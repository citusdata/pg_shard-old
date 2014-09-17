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

#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"


extern void deparse_shard_query(Query *query, int64 shardid, StringInfo buffer);


#endif /* PG_SHARD_RULEUTILS_H */
