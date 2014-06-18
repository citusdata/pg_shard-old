/*-------------------------------------------------------------------------
 *
 * pg_shard.c
 *			function definitions for pg_shard extension
 *
 * Portions Copyright (c) 2014, Citus Data, Inc.
 *
 * IDENTIFICATION
 *			hash.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"

#include "pg_shard.h"


/* declarations for dynamic loading */
PG_MODULE_MAGIC;


/*
 * _PG_init is called when the module is loaded. Just a placeholder for now.
 */
void
_PG_init(void)
{
	/* no-op */
}


/*
 * _PG_fini is called when the module is unloaded. Just a placeholder for now.
 */
void
_PG_fini(void)
{
	/* no-op */
}
