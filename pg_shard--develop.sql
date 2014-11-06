/* pg_shard--develop.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_shard" to load this file. \quit

-- the pgs_distribution_metadata schema stores data distribution information
CREATE SCHEMA pgs_distribution_metadata

	-- shard keeps track of hash value ranges for each shard
	CREATE TABLE shard (
		id bigint primary key,
		relation_id oid not null,
		storage "char" not null,
		min_value text not null,
		max_value text not null
	)

	-- shard_placement records which nodes contain which shards
	CREATE TABLE shard_placement (
		id bigint primary key,
		shard_id bigint not null references shard(id),
		shard_state integer not null,
		node_name text not null,
		node_port integer not null
	)

	-- partition lists a partition key for each distributed table
	CREATE TABLE partition (
		relation_id oid unique not null,
		partition_method "char" not null,
		key text not null
	)

	-- make a few more indexes for fast access
	CREATE INDEX shard_relation_index ON shard (relation_id)
	CREATE INDEX shard_placement_node_name_node_port_index
		ON shard_placement (node_name, node_port)
	CREATE INDEX shard_placement_shard_index ON shard_placement (shard_id)

	-- make sequences for shards and placements
	CREATE SEQUENCE shard_id_sequence
	CREATE SEQUENCE shard_placement_id_sequence;

-- mark each of the above as config tables to have pg_dump preserve them
SELECT pg_catalog.pg_extension_config_dump(
	'pgs_distribution_metadata.shard', '');
SELECT pg_catalog.pg_extension_config_dump(
	'pgs_distribution_metadata.shard_placement', '');
SELECT pg_catalog.pg_extension_config_dump(
	'pgs_distribution_metadata.partition', '');

-- define the table distribution functions
CREATE FUNCTION create_distributed_table(tablename text, partitioncolumn text,
                                         partitionmethod "char" DEFAULT 'h')
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION create_shards(tablename text, shardcount integer,
                              replicationfactor integer DEFAULT 2)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION repair_shard_placement(badnodename text, badnodeport integer,
									   goodnodename text, goodnodeport integer,
									   shardid bigint)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;
