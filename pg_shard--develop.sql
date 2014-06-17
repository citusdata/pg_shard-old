/* pg_shard/pg_shard--develop.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_shard" to load this file. \quit

CREATE FUNCTION pg_shard_hash(record, integer)
RETURNS integer
LANGUAGE C IMMUTABLE STRICT AS 'MODULE_PATHNAME';
