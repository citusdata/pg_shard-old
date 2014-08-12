/*-------------------------------------------------------------------------
 *
 * connection.c
 *		  Connection hash for pg_shard
 *
 * Portions Copyright (c) 2014, Citus Data, Inc.
 *
 * IDENTIFICATION
 *		  connection.h
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "pg_config_manual.h"
#include "postgres_ext.h"

#include "connection.h"

#include <stddef.h>
#include <string.h>

#include "commands/dbcommands.h"
#include "lib/stringinfo.h"
#include "mb/pg_wchar.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/palloc.h"


/*
 * NodeConnectionHash is the connection hash itself. It begins uninitialized.
 * The first call to GetConnection triggers hash creation.
 */
static HTAB *NodeConnectionHash = NULL;


/* local function forward declarations */
static HTAB * CreateNodeConnectionHash(void);
static PGconn * ConnectToNode(char *nodeName, int32 nodePort);
static char * ConnectionGetOptionValue(PGconn *connection, char *optionKeyword);


/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(TestPgShardConnection);


/*
 * TestPgShardConnection accepts three arguments: a hostname, port, and error
 * level. It connects to the host on the specified port and issues a RAISE at
 * the specified level.
 *
 * Intended for use in regression tests.
 */
Datum
TestPgShardConnection(PG_FUNCTION_ARGS)
{
	PGconn *connection = NULL;
	text *nodeText = PG_GETARG_TEXT_P(0);
	int32 nodePort = PG_GETARG_INT32(1);
	PGresult *result;

	char *nodeName = text_to_cstring(nodeText);

	connection = GetConnection(nodeName, nodePort);
	if (connection == NULL)
	{
		ereport(ERROR, (errmsg("could not connect to %s:%d", nodeName, nodePort)));
	}

	result = PQexec(connection, TEST_SQL);

	if (PQresultStatus(result) != PGRES_COMMAND_OK)
	{
		ReportRemoteSqlError(connection, result);
	}

	PQclear(result);

	PG_RETURN_VOID();
}


/*
 * GetConnection returns a PGconn which can be used to execute queries on a
 * remote PostgreSQL server. If no suitable connection to the specified node on
 * the specified port yet exists, the function establishes a new connection and
 * returns that.
 *
 * Returned connections are guaranteed to be in the CONNECTION_OK state. If the
 * requested connection cannot be established, or if it was previously created
 * but is now in an unrecoverable bad state, this function returns NULL.
 *
 * This function throws an error if a hostname over 255 characters is provided.
 */
PGconn *
GetConnection(char *nodeName, int32 nodePort)
{
	PGconn *connection = NULL;
	NodeConnectionKey nodeConnectionKey;
	NodeConnectionEntry *nodeConnectionEntry = NULL;
	bool entryFound = false;

	/* Check input */
	if (strnlen(nodeName, MAX_NODE_LENGTH + 1) > MAX_NODE_LENGTH)
	{
		ereport(ERROR, (errmsg("hostnames may not exceed 255 characters")));
	}

	if (NodeConnectionHash == NULL)
	{
		NodeConnectionHash = CreateNodeConnectionHash();
	}

	memset(&nodeConnectionKey, 0, sizeof(nodeConnectionKey));
	strncpy(nodeConnectionKey.nodeName, nodeName, MAX_NODE_LENGTH);
	nodeConnectionKey.nodePort = nodePort;

	nodeConnectionEntry = hash_search(NodeConnectionHash, &nodeConnectionKey,
									  HASH_FIND, &entryFound);
	if (entryFound)
	{
		connection = nodeConnectionEntry->connection;
		if (PQstatus(connection) != CONNECTION_OK)
		{
			PurgeConnection(connection);

			entryFound = false;
		}
	}

	if (!entryFound)
	{
		connection = ConnectToNode(nodeName, nodePort);
		if (connection != NULL)
		{
			nodeConnectionEntry = hash_search(NodeConnectionHash, &nodeConnectionKey,
											  HASH_ENTER, &entryFound);
			nodeConnectionEntry->connection = connection;
		}
	}

	return connection;
}


/*
 * PurgeConnection removes the given connection from the connection hash and
 * closes it using PQfinish. If our hash does not contain the given connection,
 * this method simply prints a warning and exits.
 */
void PurgeConnection(PGconn *connection)
{
	NodeConnectionKey nodeConnectionKey;
	NodeConnectionEntry *nodeConnectionEntry = NULL;
	bool entryFound = false;

	char *nodeNameString = ConnectionGetOptionValue(connection, "host");
	char *nodePortString = ConnectionGetOptionValue(connection, "port");

	if (nodeNameString != NULL && nodePortString != NULL)
	{
		int32 nodePort = pg_atoi(nodePortString, sizeof(int32), 0);

		memset(&nodeConnectionKey, 0, sizeof(nodeConnectionKey));
		strncpy(nodeConnectionKey.nodeName, nodeNameString, MAX_NODE_LENGTH);
		nodeConnectionKey.nodePort = nodePort;

		pfree(nodeNameString);
		pfree(nodePortString);
	}
	else
	{
		ereport(ERROR, (errmsg("connections must have host and port options set")));
	}

	nodeConnectionEntry = hash_search(NodeConnectionHash, &nodeConnectionKey,
									  HASH_REMOVE, &entryFound);
	if (entryFound)
	{
		/*
		 * It's possible the provided connection matches the host and port for
		 * an entry in the hash without being precisely the same connection. In
		 * that case, we will want to close the hash's connection (because the
		 * entry has already been removed) in addition to the provided one.
		 */
		if (nodeConnectionEntry->connection != connection)
		{
			ereport(WARNING, (errmsg("hash entry for %s:%d contained different "
									 "connection than that provided by caller",
									 nodeConnectionKey.nodeName,
									 nodeConnectionKey.nodePort)));
			PQfinish(nodeConnectionEntry->connection);
		}
	}
	else
	{
		ereport(WARNING, (errmsg("could not find hash entry for connection to %s:%d",
								 nodeConnectionKey.nodeName,
								 nodeConnectionKey.nodePort)));
	}

	PQfinish(connection);
}


/*
 * ReportRemoteSqlError retrieves various error fields from the a remote result
 * and produces an error report at the WARNING level.
 */
void
ReportRemoteSqlError(PGconn *connection, PGresult *result)
{
	char *sqlStateString = PQresultErrorField(result, PG_DIAG_SQLSTATE);
	char *remoteMessage = PQresultErrorField(result, PG_DIAG_MESSAGE_PRIMARY);
	int sqlState = ERRCODE_CONNECTION_FAILURE;
	char *nodeName = ConnectionGetOptionValue(connection, "host");
	char *nodePort = ConnectionGetOptionValue(connection, "port");
	char *errorPrefix = "Bad result from";

	if (sqlStateString != NULL)
	{
		sqlState = MAKE_SQLSTATE(sqlStateString[0], sqlStateString[1], sqlStateString[2],
								 sqlStateString[3], sqlStateString[4]);
	}

	/* Use more specific error prefix for connection failures */
	if (sqlState == ERRCODE_CONNECTION_FAILURE)
	{
		errorPrefix = "Connection failed to";
	}

	/*
	 * If the PGresult did not contain a message, the connection may provide a
	 * suitable top level one. At worst, this is an empty string.
	 */
	if (remoteMessage == NULL)
	{
		remoteMessage = PQerrorMessage(connection);
	}

	ereport(WARNING, (errcode(sqlState),
					  errmsg("%s %s:%s", errorPrefix, nodeName, nodePort),
					  errdetail("Remote message: %s", remoteMessage)));
}


/*
 * CreateNodeConnectionHash returns a newly created hash table suitable for
 * storing unlimited connections indexed by node name and port.
 */
static HTAB *
CreateNodeConnectionHash(void)
{
	HTAB *nodeConnectionHash = NULL;
	HASHCTL info;
	int hashFlags = 0;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(NodeConnectionKey);
	info.entrysize = sizeof(NodeConnectionEntry);
	info.hash = tag_hash;
	info.hcxt = CacheMemoryContext;
	hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	nodeConnectionHash = hash_create("pg_shard connections", 32, &info, hashFlags);

	return nodeConnectionHash;
}


/*
 * ConnectToNode opens a connection to a remote PostgreSQL server. The function
 * configures the connection's fallback application name to 'pg_shard' and sets
 * the remote encoding to match the local one.
 *
 * We attempt to connect up to MAX_CONNECT_ATTEMPT times. After that we give up
 * and return NULL.
 */
static PGconn *
ConnectToNode(char *nodeName, int32 nodePort)
{
	PGconn *connection = NULL;
	StringInfo nodePortString = makeStringInfo();
	appendStringInfo(nodePortString, "%d", nodePort);

	const char *keywordArray[] = { "host", "port", "fallback_application_name",
			"client_encoding", "connect_timeout",
			"dbname", NULL };
	const char *valueArray[] = { nodeName, nodePortString->data, "pg_shard",
			GetDatabaseEncodingName(), CLIENT_CONNECT_TIMEOUT_SECONDS,
			get_database_name(MyDatabaseId), NULL };

	Assert(sizeof(keywordArray) == sizeof(valueArray));

	for (int attemptsMade = 0; attemptsMade < MAX_CONNECT_ATTEMPTS; attemptsMade++)
	{
		connection = PQconnectdbParams(keywordArray, valueArray, false);
		if (PQstatus(connection) == CONNECTION_OK)
		{
			break;
		}
		else
		{
			PQfinish(connection);
			connection = NULL;
		}
	}

	pfree(nodePortString->data);
	pfree(nodePortString);

	return connection;
}


/*
 * ConnectionGetOptionValue inspects the provided connection for an option with
 * a given keyword and returns a new palloc'd string with that options's value.
 * The function returns NULL if the connection has no setting for an option with
 * the provided keyword.
 */
static char *
ConnectionGetOptionValue(PGconn *connection, char *optionKeyword)
{
	char *optionValue = NULL;
	PQconninfoOption *conninfoOptions = PQconninfo(connection);

	for (PQconninfoOption *option = conninfoOptions; option->keyword != NULL; option++)
	{
		if (strncmp(option->keyword, optionKeyword, NAMEDATALEN) == 0)
		{
			optionValue = pstrdup(option->val);
		}
	}

	PQconninfoFree(conninfoOptions);

	return optionValue;
}
