/*-------------------------------------------------------------------------
 *
 * test.h
 *		  Test wrapper functions for pg_shard
 *
 * Portions Copyright (c) 2014, Citus Data, Inc.
 *
 * IDENTIFICATION
 *		  test.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_SHARD_TEST_H
#define PG_SHARD_TEST_H

#include "postgres.h"
#include "fmgr.h"

/* SQL statement for testing */
#define TEST_SQL "DO $$ BEGIN RAISE EXCEPTION 'Raised remotely!'; END $$"

extern Datum TestDistributionMetadata(PG_FUNCTION_ARGS);
extern Datum TestPgShardConnection(PG_FUNCTION_ARGS);

#endif /* PG_SHARD_TEST_H */
