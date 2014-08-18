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

#define SHARDID_SEQUENCE_NAME "shardid_sequence"
#define WHITESPACE " \t\n\r"
#define WORKER_LIST_FILENAME "pg_worker_list.conf"

/* Transaction related commands used in talking to the worker nodes. */
#define BEGIN_COMMAND "BEGIN"
#define COMMIT_COMMAND "COMMIT"
#define ROLLBACK_COMMAND "ROLLBACK"

/* function declarations for initializing a distributed table */
extern Datum master_create_distributed_table(PG_FUNCTION_ARGS);


#endif /* PARTITION_PROTOCOL_H */
