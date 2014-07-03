-- ===================================================================
-- create test functions
-- ===================================================================

CREATE FUNCTION test_pg_shard_connection(text, integer, text)
	RETURNS void
	AS 'pg_shard', 'TestPgShardConnection'
	LANGUAGE C STRICT;

-- ===================================================================
-- test connection hash functionality
-- ===================================================================

-- test connection to localhost raising an info message
SELECT test_pg_shard_connection('', 5432, 'INFO');

-- test connection to localhost raising an exception
SELECT test_pg_shard_connection('', 5432, 'EXCEPTION');

-- test connection to non-existant machine
SELECT test_pg_shard_connection('dummyhost', 5432, 'INFO');
