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

#ifndef DDL_COMMANDS_H
#define DDL_COMMANDS_H

#include "c.h"
#include "fmgr.h"
#include "libpq-fe.h"
#include "postgres_ext.h"

#include "lib/stringinfo.h"
#include "nodes/pg_list.h"


#define SHARD_NAME_SEPARATOR '_'

extern List * TableDDLCommandList(Oid relationId);
extern void AppendOptionListToString(StringInfo stringBuffer, List *optionList);
extern List * ExtendedDDLCommandList(Oid masterRelationId, uint64 shardId,
									 List *ddlCommandList);
extern bool ExecuteRemoteCommandList(char *nodeName, uint32 nodePort,
									 List *ddlCommandList);


#endif /* DDL_COMMANDS_H */
