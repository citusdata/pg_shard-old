/*-------------------------------------------------------------------------
 *
 * partition_protocol.h
 *			pg_shard function declarations to create partitions
 *
 * Portions Copyright (c) 2014, Citus Data, Inc.
 *
 * IDENTIFICATION
 *			partition_protocol.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PARTITION_PROTOCOL_H
#define PARTITION_PROTOCOL_H

#include "fmgr.h"

#define SHARDID_SEQUENCE_NAME "shardid_sequence"
#define MAX_WORKER_NODE_LINE 8192
#define WHITESPACE " \t\n\r"
#define WORKER_LIST_FILENAME "pg_worker_list.conf"


/* function declarations for initializing a distributed table */
extern Datum master_create_distributed_table(PG_FUNCTION_ARGS);


#endif /* PARTITION_PROTOCOL_H */
