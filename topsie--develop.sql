/* topsie--develop.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION topsie" to load this file. \quit

CREATE FUNCTION topsie_hash(record, integer)
RETURNS integer
LANGUAGE C IMMUTABLE STRICT AS 'MODULE_PATHNAME';

CREATE SCHEMA topsie_metadata
	CREATE TABLE shards (
		id bigserial primary key,
		relation_id oid not null,
		min_value integer not null,
		max_value integer not null
	)

	CREATE TABLE placements (
		id bigserial,
		shard_id bigint references shards,
		host text not null,
		port integer not null
	);

CREATE FUNCTION topsie_print_metadata(oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION postgres_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION topsie_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER topsie
  HANDLER postgres_fdw_handler
  VALIDATOR topsie_validator;
