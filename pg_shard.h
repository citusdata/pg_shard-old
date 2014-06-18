/*-------------------------------------------------------------------------
 *
 * pg_shard.h
 *			type and function declarations for pg_shard extension
 *
 * Portions Copyright (c) 2014, Citus Data, Inc.
 *
 * IDENTIFICATION
 *			pg_shard.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_SHARD_H
#define PG_SHARD_H


/* Function declarations for extension loading and unloading */
extern void _PG_init(void);
extern void _PG_fini(void);


#endif /* PG_SHARD_H */
