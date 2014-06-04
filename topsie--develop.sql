/* topsie--develop.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION topsie" to load this file. \quit

CREATE FUNCTION topsie_hash(record, integer)
RETURNS integer
LANGUAGE C IMMUTABLE STRICT AS 'MODULE_PATHNAME';

CREATE FUNCTION postgres_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION postgres_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER topsie
  HANDLER postgres_fdw_handler
  VALIDATOR postgres_fdw_validator;
