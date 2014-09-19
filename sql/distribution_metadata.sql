-- ===================================================================
-- create test functions
-- ===================================================================

CREATE FUNCTION load_shard_id_array(oid)
	RETURNS bigint[]
	AS 'pg_shard'
	LANGUAGE C STRICT;

CREATE FUNCTION load_shard_interval_array(bigint)
	RETURNS integer[]
	AS 'pg_shard'
	LANGUAGE C STRICT;

CREATE FUNCTION load_shard_placement_array(bigint)
	RETURNS text[]
	AS 'pg_shard'
	LANGUAGE C STRICT;

CREATE FUNCTION partition_column_attribute_number(oid)
	RETURNS smallint
	AS 'pg_shard'
	LANGUAGE C STRICT;

-- ===================================================================
-- test distribution metadata functionality
-- ===================================================================


CREATE TEMPORARY TABLE events (
	id bigint,
	name text
);

INSERT INTO pgs_distribution_metadata.shard
	(id, relation_id, storage, min_value, max_value)
VALUES
	(1, 'events'::regclass, 't', '0', '10'),
	(2, 'events'::regclass, 't', '10', '20'),
	(3, 'events'::regclass, 't', '20', '30'),
	(4, 'events'::regclass, 't', '30', '40');

INSERT INTO pgs_distribution_metadata.shard_placement
	(id, node_name, node_port, shard_id, shard_state)
VALUES
	(1, 'foo', 123, 1, 0),
	(2, 'foo', 123, 2, 0),
	(3, 'bar', 456, 3, 0),
	(4, 'bar', 456, 4, 0),
	(5, 'baz', 123, 1, 0),
	(6, 'baz', 123, 2, 0),
	(7, 'qux', 456, 3, 0),
	(8, 'qux', 456, 4, 0);

INSERT INTO pgs_distribution_metadata.partition (relation_id, partition_method, key)
VALUES
	('events'::regclass, 'h', 'name');

-- should see above shard identifiers
SELECT load_shard_id_array('events'::regclass);

-- should see empty array (catalog is not distributed)
SELECT load_shard_id_array('pg_type'::regclass);

-- should see array with first shard range
SELECT load_shard_interval_array(1);

-- should see error for non-existent shard
SELECT load_shard_interval_array(5);

-- should see two placements
SELECT load_shard_placement_array(2);

-- should see error for non-existent shard
SELECT load_shard_placement_array(6);

-- should see attribute number of 'name'
SELECT partition_column_attribute_number('events'::regclass);

-- should see error (catalog is not distributed)
SELECT partition_column_attribute_number('pg_type'::regclass);
