/*-------------------------------------------------------------------------
 *
 * hash.h
 *			pg_shard function declarations to hash partition column values
 *
 * Portions Copyright (c) 2014, Citus Data, Inc.
 *
 * IDENTIFICATION
 *			hash.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef HASH_H
#define HASH_H

#include "fmgr.h"

/* Function declaration for hashing an input row */
extern Datum pg_shard_hash(PG_FUNCTION_ARGS);

#endif /* HASH_H */
