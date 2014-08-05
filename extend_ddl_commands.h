/*-------------------------------------------------------------------------
 *
 * extend_ddl_events.h
 *			function declarations to extend ddl commands for a table
 *
 * Portions Copyright (c) 2014, Citus Data, Inc.
 *
 * IDENTIFICATION
 *			extend_ddl_events.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef EXTEND_DDL_EVENTS_H
#define EXTEND_DDL_EVENTS_H

#include "fmgr.h"
#include "nodes/pg_list.h"


#define SHARD_NAME_SEPARATOR '_'

/* function for extending ddl commands */
extern List * ExtendedDDLCommandList(Oid masterRelationId, uint64 shardId,
									 List *ddlCommandList);


#endif /* EXTEND_DDL_EVENTS_H */
