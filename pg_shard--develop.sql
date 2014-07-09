/* pg_shard--develop.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_shard" to load this file. \quit

-- the pgs_distribution_metadata schema stores data distribution information
CREATE SCHEMA pgs_distribution_metadata

	-- shard keeps track of hash value ranges for each shard
	CREATE TABLE shard (
		id bigserial primary key,
		relation_id oid not null,
		min_value text not null,
		max_value text not null
	)

	-- shard_placement records which nodes contain which shards
	CREATE TABLE shard_placement (
		id bigserial primary key,
		shard_id bigint not null references shard,
		node_name text not null,
		node_port integer not null
	)

	-- partition_strategy lists a partition key for each distributed table
	CREATE TABLE partition_strategy (
		relation_id oid unique not null,
		key text not null
	)

	-- make a few more indexes for fast access
	CREATE INDEX shard_idx_relation_id ON shard (relation_id)
	CREATE INDEX placement_node_idx_node_name_node_port
		ON shard_placement (node_name, node_port)
	CREATE INDEX placement_idx_shard_id ON shard_placement (shard_id);

-- mark each of the above as config tables to have pg_dump preserve them
SELECT pg_catalog.pg_extension_config_dump(
	'pgs_distribution_metadata.shard', '');
SELECT pg_catalog.pg_extension_config_dump(
	'pgs_distribution_metadata.shard_placement', '');
SELECT pg_catalog.pg_extension_config_dump(
	'pgs_distribution_metadata.partition_strategy', '');
