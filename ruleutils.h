/*-------------------------------------------------------------------------
 *
 * ruleutils.h
 *
 * Declarations for public functions and types related to extending a query to
 * refer to a shard instead of the original table, and deparse it into its SQL
 * form.
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
