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

INSERT INTO pgs_distribution_metadata.shard
	(id, relation_id, min_value, max_value)
VALUES
	(1, 'events'::regclass, '0', '10'),
	(2, 'events'::regclass, '10', '20'),
	(3, 'events'::regclass, '20', '30'),
	(4, 'events'::regclass, '30', '40');

INSERT INTO pgs_distribution_metadata.shard_placement
	(id, node_name, node_port, shard_id, shard_state)
VALUES
	(1, 'foo', 123, 1, 0),
	(2, 'foo', 123, 2, 0),
	(3, 'bar', 456, 3, 0),
	(4, 'bar', 456, 4, 0);

INSERT INTO pgs_distribution_metadata.partition (relation_id, key)
VALUES
	('events'::regclass, 'name');

SELECT test_distribution_metadata('events'::regclass);
