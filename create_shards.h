/*-------------------------------------------------------------------------
 *
 * partition_protocol.h
 *			pg_shard function declarations to create partitions
 *
 * Copyright (c) 2014, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#ifndef PARTITION_PROTOCOL_H
#define PARTITION_PROTOCOL_H

#include "fmgr.h"

#define WORKER_LIST_FILENAME "pg_worker_list.conf"

/* transaction related commands used in talking to the worker nodes */
#define BEGIN_COMMAND "BEGIN"
#define COMMIT_COMMAND "COMMIT"
#define ROLLBACK_COMMAND "ROLLBACK"


/* In-memory representation of a worker node */
typedef struct WorkerNode
{
	uint32 nodePort;
	char *nodeName;

} WorkerNode;


/* function declarations for initializing a distributed table */
extern Datum create_distributed_table(PG_FUNCTION_ARGS);
extern Datum create_shards(PG_FUNCTION_ARGS);


#endif /* PARTITION_PROTOCOL_H */
