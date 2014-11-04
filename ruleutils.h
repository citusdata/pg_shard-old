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
#include "postgres_ext.h"

#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"


extern void deparse_shard_query(Query *query, int64 shardid, StringInfo buffer);
extern char *generate_shard_name(Oid relid, int64 shardid);


#endif /* PG_SHARD_RULEUTILS_H */
