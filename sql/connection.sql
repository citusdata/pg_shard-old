-- ===================================================================
-- create test functions
-- ===================================================================

CREATE FUNCTION test_pg_shard_connection(text, integer)
	RETURNS void
	AS 'pg_shard', 'TestPgShardConnection'
	LANGUAGE C STRICT;

-- ===================================================================
-- test connection hash functionality
-- ===================================================================

-- test connection to localhost raising an exception
SELECT test_pg_shard_connection('localhost', 5432);

-- test connection to non-existant machine
SELECT test_pg_shard_connection('dummyhost', 5432);
