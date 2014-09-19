/*-------------------------------------------------------------------------
 *
 * test_helper_functions.h
 *		  Test wrapper functions for pg_shard
 *
 * Portions Copyright (c) 2014, Citus Data, Inc.
 *
 * IDENTIFICATION
 *		  test_helper_functions.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_SHARD_TEST_HELPER_H
#define PG_SHARD_TEST_HELPER_H

#include "postgres.h"
#include "fmgr.h"


/* SQL statements for testing */
#define POPULATE_TEMP_TABLE "CREATE TEMPORARY TABLE numbers " \
							"AS SELECT * FROM generate_series(1, 100);"
#define COUNT_TEMP_TABLE	"SELECT COUNT(*) FROM numbers;"


extern Datum initialize_remote_temp_table(PG_FUNCTION_ARGS);
extern Datum count_remote_temp_table_rows(PG_FUNCTION_ARGS);
extern Datum get_and_purge_connection(PG_FUNCTION_ARGS);
extern Datum load_shard_id_array(PG_FUNCTION_ARGS);
extern Datum load_shard_interval_array(PG_FUNCTION_ARGS);
extern Datum load_shard_placement_array(PG_FUNCTION_ARGS);
extern Datum partition_column_attribute_number(PG_FUNCTION_ARGS);


#endif /* PG_SHARD_TEST_HELPER_H */
