/* contrib/topsie/topsie--develop.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION topsie" to load this file. \quit

CREATE FUNCTION topsie_hash(record, integer)
RETURNS integer
LANGUAGE C IMMUTABLE STRICT AS 'MODULE_PATHNAME';
