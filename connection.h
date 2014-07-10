/*-------------------------------------------------------------------------
 *
 * connection.h
 *		  Connection hash for pg_shard
 *
 * Portions Copyright (c) 2014, Citus Data, Inc.
 *
 * IDENTIFICATION
 *		  connection.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_SHARD_CONNECTION_H
#define PG_SHARD_CONNECTION_H

#include "postgres.h"
#include "c.h"
#include "fmgr.h"
#include "libpq-fe.h"


extern PGconn * GetConnection(char *nodeName, int32 nodePort);
extern Datum TestPgShardConnection(PG_FUNCTION_ARGS);

#endif /* PG_SHARD_CONNECTION_H */
