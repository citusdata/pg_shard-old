/*-------------------------------------------------------------------------
 *
 * generate_ddl_events.h
 *			function declarations to generate ddl commands for a table
 *
 * Portions Copyright (c) 2014, Citus Data, Inc.
 *
 * IDENTIFICATION
 *			generate_ddl_events.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef GENERATE_DDL_EVENTS_H
#define GENERATE_DDL_EVENTS_H

#include "fmgr.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"


/* function for generating ddl commands */
extern List * TableDDLCommandList(Oid relationId);
extern void AppendOptionListToString(StringInfo stringBuffer, List *optionList);


#endif /* GENERATE_DDL_EVENTS_H */
