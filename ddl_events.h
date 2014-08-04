/*-------------------------------------------------------------------------
 *
 * ddl_events.h
 *			function declarations to retrieve and extend ddl events for a table
 *
 * Portions Copyright (c) 2014, Citus Data, Inc.
 *
 * IDENTIFICATION
 *			ddl_events.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef DDL_EVENTS_H
#define DDL_EVENTS_H

#include "fmgr.h"
#include "nodes/pg_list.h"

#define SHARD_NAME_SEPARATOR '_'

/* functions for constructing and extending ddl events */
extern List * GetTableDDLEvents(Oid relationId);
extern List * ExtendedTableDDLEvents(List *ddlEventList, uint64 shardId,
									 Oid originalRelationId);


#endif /* DDL_EVENTS_H */
