/*-------------------------------------------------------------------------
 *
 * ddl_commands.h
 *			function declarations to generate and extend ddl commands for a
 *			table.
 *
 * Copyright (c) 2014, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#ifndef DDL_COMMANDS_H
#define DDL_COMMANDS_H

#include "fmgr.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"


#define SHARD_NAME_SEPARATOR '_'

extern List * TableDDLCommandList(Oid relationId);
extern void AppendOptionListToString(StringInfo stringBuffer, List *optionList);
extern List * ExtendedDDLCommandList(Oid masterRelationId, uint64 shardId,
									 List *ddlCommandList);


#endif /* DDL_COMMANDS_H */
