/*-------------------------------------------------------------------------
 *
 * ruleutils.h
 *
 * Declarations for public functions and types to produce an SQL string
 * targeting a particular shard based on an initial query and shard ID.
 *
 * Copyright (c) 2014, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_SHARD_RULEUTILS_H
#define PG_SHARD_RULEUTILS_H

#include "c.h"

#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"


/* function declarations for extending and deparsing a query */
extern void deparse_shard_query(Query *query, int64 shardid, StringInfo buffer);


#endif /* PG_SHARD_RULEUTILS_H */
