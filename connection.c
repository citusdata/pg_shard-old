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
#include "postgres_ext.h"

#include "connection.h"

#include <stddef.h>
#include <string.h>

#include "commands/dbcommands.h"
#include "lib/stringinfo.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/palloc.h"


/* Maximum duration to wait for connection */
#define CLIENT_CONNECT_TIMEOUT_SECONDS "5"

/* Maximum (textual) lengths of hostname */
#define MAX_NODE_LENGTH 255

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

/*
 * NodeConnectionHash is the connection hash itself. It begins uninitialized.
 * The first call to GetConnection triggers hash creation.
 */
static HTAB *NodeConnectionHash = NULL;


/* local function forward declarations */
static HTAB * CreateNodeConnectionHash(void);
static PGconn * ConnectToNode(char *nodeName, int32 nodePort);


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
	text *elevelText = PG_GETARG_TEXT_P(2);
	PGresult *result;

	char *nodeName = text_to_cstring(nodeText);
	char *elevel = text_to_cstring(elevelText);

	StringInfoData sqlCommand;
	initStringInfo(&sqlCommand);

	appendStringInfo(&sqlCommand, TEST_SQL, elevel);

	connection = GetConnection(nodeName, nodePort);

	result = PQexec(connection, sqlCommand.data);

	if (PQresultStatus(result) != PGRES_COMMAND_OK)
	{
		ReportRemoteSqlError(ERROR, result, connection, true, sqlCommand.data);
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
 * Hostnames may not be longer than 255 characters.
 */
PGconn *
GetConnection(char *nodeName, int32 nodePort)
{
	PGconn *connection = NULL;
	NodeConnectionKey nodeConnectionKey;
	NodeConnectionEntry *nodeConnectionEntry = NULL;
	bool entryFound = false;

	memset(&nodeConnectionKey, 0, sizeof(nodeConnectionKey));
	strncpy(nodeConnectionKey.nodeName, nodeName, MAX_NODE_LENGTH);
	nodeConnectionKey.nodePort = nodePort;

	if (NodeConnectionHash == NULL)
	{
		NodeConnectionHash = CreateNodeConnectionHash();
	}

	nodeConnectionEntry = hash_search(NodeConnectionHash, &nodeConnectionKey,
									  HASH_FIND, &entryFound);
	if (entryFound)
	{
		connection = nodeConnectionEntry->connection;
	}
	else
	{
		connection = ConnectToNode(nodeConnectionKey.nodeName, nodePort);

		nodeConnectionEntry = hash_search(NodeConnectionHash, &nodeConnectionKey,
										  HASH_ENTER, &entryFound);
		nodeConnectionEntry->connection = connection;
	}

	return connection;
}


/*
 * ReportRemoteSqlError retrieves various error fields from the a remote result
 * and reports an error at the specified level. Callers should provide this
 * function with the actual SQL command previously sent to the remote server in
 * order to have it included in the user-facing error context string.
 */
void
ReportRemoteSqlError(int errorLevel, PGresult *result, PGconn *connection,
					 bool clearResult, const char *sqlCommand)
{
	char *sqlStateString = PQresultErrorField(result, PG_DIAG_SQLSTATE);
	char *primaryMessage = PQresultErrorField(result, PG_DIAG_MESSAGE_PRIMARY);
	char *messageDetail = PQresultErrorField(result, PG_DIAG_MESSAGE_DETAIL);
	char *messageHint = PQresultErrorField(result, PG_DIAG_MESSAGE_HINT);
	char *errorContext = PQresultErrorField(result, PG_DIAG_CONTEXT);
	int sqlState = ERRCODE_CONNECTION_FAILURE;

	if (sqlStateString != NULL)
	{
		sqlState = MAKE_SQLSTATE(sqlStateString[0], sqlStateString[1], sqlStateString[2],
								 sqlStateString[3], sqlStateString[4]);
	}

	/*
	 * If the PGresult did not contain a message, the connection may provide a
	 * suitable top level one.
	 */
	if (primaryMessage == NULL)
	{
		primaryMessage = PQerrorMessage(connection);
	}

	/* Fall back to a default */
	if (primaryMessage == NULL)
	{
		primaryMessage = "unknown error";
	}

	if (clearResult)
	{
		PQclear(result);
	}

	ereport(errorLevel,
		(
			errcode(sqlState),
			errmsg_internal("%s", primaryMessage),
			(messageDetail ? errdetail_internal("%s", messageDetail)          : 0),
			(messageHint   ? errhint("%s", messageHint)                       : 0),
			(errorContext  ? errcontext("%s", errorContext)                   : 0),
			(sqlCommand    ? errcontext("Remote SQL command: %s", sqlCommand) : 0)
		)
	);
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
 */
static PGconn *
ConnectToNode(char *nodeName, int32 nodePort)
{
	PGconn *connection = NULL;
	StringInfoData nodePortString;
	initStringInfo(&nodePortString);
	appendStringInfo(&nodePortString, "%d", nodePort);

	const char *keywordArray[] = {
			"host",
			"port",
			"fallback_application_name",
			"client_encoding",
			"connect_timeout",
			"dbname",
			NULL
	};
	const char *valueArray[] = {
			nodeName,
			nodePortString.data,
			"pg_shard",
			GetDatabaseEncodingName(),
			CLIENT_CONNECT_TIMEOUT_SECONDS,
			get_database_name(MyDatabaseId),
			NULL
	};

	Assert(sizeof(keywordArray) == sizeof(keywordArray));
	connection = PQconnectdbParams(keywordArray, valueArray, false);

	if (PQstatus(connection) != CONNECTION_OK)
	{
		char *connectionMessage;
		char *newline;

		/* strip libpq-appended newline, if any */
		connectionMessage = pstrdup(PQerrorMessage(connection));
		newline = strrchr(connectionMessage, '\n');
		if (newline != NULL)
		{
			*newline = '\0';
		}

		PQfinish(connection);

		ereport(ERROR, (errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
						errmsg("could not connect to node \"%s\" (on port "
							   "%d)", nodeName, nodePort),
						errdetail_internal("%s", connectionMessage)));
	}

	return connection;
}
