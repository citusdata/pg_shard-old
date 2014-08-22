-- ===================================================================
-- create test functions
-- ===================================================================

CREATE FUNCTION test_distribution_metadata(oid)
	RETURNS void
	AS 'pg_shard', 'TestDistributionMetadata'
	LANGUAGE C STRICT;

CREATE FUNCTION load_shard_id_array(oid)
	RETURNS bigint[]
	AS 'pg_shard', 'LoadShardIdArray'
	LANGUAGE C STRICT;

-- ===================================================================
-- test distribution metadata functionality
-- ===================================================================


CREATE TEMPORARY TABLE events (
	id bigint,
	name text
);

CREATE TEMPORARY TABLE customers (
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
	(4, 'bar', 456, 4, 0);

INSERT INTO pgs_distribution_metadata.partition (relation_id, partition_method, key)
VALUES
	('events'::regclass, 'h', 'name');

SELECT test_distribution_metadata('events'::regclass);

-- should see above shard identifiers
SELECT load_shard_id_array('events'::regclass);

-- should see empty array (no distribution)
SELECT load_shard_id_array('customers'::regclass);
