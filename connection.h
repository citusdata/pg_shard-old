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


/* Maximum duration to wait for connection */
#define CLIENT_CONNECT_TIMEOUT_SECONDS "5"

/* Maximum (textual) lengths of hostname */
#define MAX_NODE_LENGTH 255

/* Times to attempt connection (or reconnection) */
#define MAX_CONNECT_ATTEMPTS 2

/* SQL statement for testing */
#define TEST_SQL "DO $$ BEGIN RAISE %s 'Raised remotely!'; END $$"

/*
 * NodeConnectionKey acts as the key to index into the (process-local) hash
 * keeping track of open connections. Node name and port are sufficient.
 */
typedef struct NodeConnectionKey
{
	char nodeName[MAX_NODE_LENGTH + 1];	/* hostname of host to connect to */
	int32 nodePort;						/* port of host to connect to */
} NodeConnectionKey;


/* NodeConnectionEntry keeps track of connections themselves. */
typedef struct NodeConnectionEntry
{
	NodeConnectionKey cacheKey;	/* hash entry key */
	PGconn *connection;			/* connection to remote server, if any */
} NodeConnectionEntry;


extern PGconn * GetConnection(char *nodeName, int32 nodePort);
extern void PurgeConnection(PGconn *connection);
extern void ReportRemoteSqlError(int errorLevel, PGresult *result, PGconn *connection,
								 bool clearResult, const char *sqlCommand);
extern Datum TestPgShardConnection(PG_FUNCTION_ARGS);

#endif /* PG_SHARD_CONNECTION_H */
