/*-------------------------------------------------------------------------
 *
 * ddl_commands.h
 *
 * Function declarations to generate and extend ddl commands for a table.
 *
 * Copyright (c) 2014, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_SHARD_DDL_COMMANDS_H
#define PG_SHARD_DDL_COMMANDS_H

#include "c.h"
#include "fmgr.h"
#include "postgres_ext.h"

#include "lib/stringinfo.h"
#include "nodes/pg_list.h"


/* separator between tablename and shardId */
#define SHARD_NAME_SEPARATOR '_'


/* Function declarations to extend ddl commands with shardId's */
extern List * TableDDLCommandList(Oid relationId);
extern void AppendOptionListToString(StringInfo stringBuffer, List *optionList);
extern List * ExtendedDDLCommandList(Oid masterRelationId, uint64 shardId,
									 List *sqlCommandList);
extern void AppendShardIdToName(char **name, uint64 shardId);
extern bool ExecuteRemoteCommandList(char *nodeName, uint32 nodePort,
									 List *sqlCommandList);


#endif /* PG_SHARD_DDL_COMMANDS_H */
