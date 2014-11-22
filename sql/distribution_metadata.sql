-- ===================================================================
-- create test functions
-- ===================================================================

CREATE FUNCTION load_shard_id_array(regclass)
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

CREATE FUNCTION partition_column_id(regclass)
	RETURNS smallint
	AS 'pg_shard'
	LANGUAGE C STRICT;

-- ===================================================================
-- test distribution metadata functionality
-- ===================================================================

-- set up a table and "distribute" it manually
CREATE TABLE events (
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
	(101, 'cluster-worker-01', 5432, 1, 0),
	(102, 'cluster-worker-01', 5432, 2, 0),
	(103, 'cluster-worker-02', 5433, 3, 0),
	(104, 'cluster-worker-02', 5433, 4, 0),
	(105, 'cluster-worker-03', 5434, 1, 0),
	(106, 'cluster-worker-03', 5434, 2, 0),
	(107, 'cluster-worker-04', 5435, 3, 0),
	(108, 'cluster-worker-04', 5435, 4, 0);

INSERT INTO pgs_distribution_metadata.partition (relation_id, partition_method, key)
VALUES
	('events'::regclass, 'h', 'name');

-- should see above shard identifiers
SELECT load_shard_id_array('events');

-- should see empty array (catalog is not distributed)
SELECT load_shard_id_array('pg_type');

-- should see array with first shard range
SELECT load_shard_interval_array(1);

-- should see error for non-existent shard
SELECT load_shard_interval_array(5);

-- should see two placements
SELECT load_shard_placement_array(2);

-- should see error for non-existent shard
SELECT load_shard_placement_array(6);

-- should see column id of 'name'
SELECT partition_column_id('events');

-- should see error (catalog is not distributed)
SELECT partition_column_id('pg_type');

-- clean up after ourselves
DROP TABLE events;
