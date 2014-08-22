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


/* SQL statements for testing */
#define POPULATE_TEMP_TABLE "CREATE TEMPORARY TABLE numbers " \
							"AS SELECT * FROM generate_series(1, 100);"
#define COUNT_TEMP_TABLE "SELECT COUNT(*) FROM numbers;"


extern Datum PopulateTempTable(PG_FUNCTION_ARGS);
extern Datum CountTempTable(PG_FUNCTION_ARGS);
extern Datum GetAndPurgeConnection(PG_FUNCTION_ARGS);
extern Datum TestDistributionMetadata(PG_FUNCTION_ARGS);
extern Datum LoadShardIdArray(PG_FUNCTION_ARGS);


#endif /* PG_SHARD_TEST_H */
