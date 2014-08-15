-- ===================================================================
-- create test functions
-- ===================================================================

CREATE FUNCTION populate_temp_table(text, integer)
	RETURNS void
	AS 'pg_shard', 'PopulateTempTable'
	LANGUAGE C STRICT;

CREATE FUNCTION count_temp_table(text, integer)
	RETURNS integer
	AS 'pg_shard', 'CountTempTable'
	LANGUAGE C STRICT;

CREATE FUNCTION get_and_purge_connection(text, integer)
	RETURNS void
	AS 'pg_shard', 'GetAndPurgeConnection'
	LANGUAGE C STRICT;

-- ===================================================================
-- test connection hash functionality
-- ===================================================================

-- connect to non-existant host
SELECT populate_temp_table('dummyhost', 5432);

-- try to use hostname over 255 characters
SELECT populate_temp_table(repeat('a', 256), 5432);

-- connect to localhost and build a temp table
SELECT populate_temp_table('localhost', 5432);

-- table should still be visible since session is reused
SELECT count_temp_table('localhost', 5432);

-- purge existing connection to localhost
SELECT get_and_purge_connection('localhost', 5432);

-- should not be able to see table anymore
SELECT count_temp_table('localhost', 5432);

-- recreate once more
SELECT populate_temp_table('localhost', 5432);

-- kill backend to disconnect
SELECT pg_terminate_backend(pid)
	FROM pg_stat_activity
	WHERE application_name = 'pg_shard';

-- should get connection failure (cached connection bad)
SELECT count_temp_table('localhost', 5432);

-- should get result failure (reconnected, so no temp table)
SELECT count_temp_table('localhost', 5432);
