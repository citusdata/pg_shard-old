-- ===================================================================
-- create test functions
-- ===================================================================

CREATE FUNCTION test_distribution_metadata(oid)
	RETURNS void
	AS 'pg_shard', 'TestDistributionMetadata'
	LANGUAGE C STRICT;


-- ===================================================================
-- test distribution metadata functionality
-- ===================================================================


CREATE TABLE events (
	id bigserial,
	name text
);

INSERT INTO pgs_metadata.shard (relation_id, min_value, max_value) VALUES
	('events'::regclass, '0', '10'),
	('events'::regclass, '10', '20'),
	('events'::regclass, '20', '30'),
	('events'::regclass, '30', '40');

INSERT INTO pgs_metadata.placement (node_name, node_port, shard_id) VALUES
	('foo', 123, 1),
	('foo', 123, 2),
	('bar', 456, 3),
	('bar', 456, 4);

INSERT INTO pgs_metadata.partition_strategy (relation_id, key) VALUES
	('events'::regclass, 'name');

SELECT test_distribution_metadata('events'::regclass);
