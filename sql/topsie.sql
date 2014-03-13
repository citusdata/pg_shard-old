--
--  Test topsie extension
--

CREATE EXTENSION topsie; -- fail, must install postgres_fdw first
CREATE EXTENSION postgres_fdw;

CREATE EXTENSION topsie SCHEMA public; -- fail, topsie is not relocatable

CREATE EXTENSION topsie;

-- Create shards for loopback server
CREATE TABLE widgets_s0 (
	c1 int PRIMARY KEY,
	c2 text
);
INSERT INTO topsie.dist_shards (shard_id, table_name) VALUES (0, 'widgets');

CREATE TABLE widgets_s1 (
	c1 int PRIMARY KEY,
	c2 text
);
INSERT INTO topsie.dist_shards (shard_id, table_name) VALUES (1, 'widgets');

-- Create main table on master
CREATE TABLE widgets (
	c1 int NOT NULL,
	c2 text
);

-- "Place" shards on loopback servers
INSERT INTO topsie.dist_shard_placements VALUES ('', (SELECT setting::integer FROM pg_settings WHERE name='port'), 1);
INSERT INTO topsie.dist_shard_placements VALUES ('', (SELECT setting::integer FROM pg_settings WHERE name='port'), 2);

-- Create servers
SELECT topsie.create_servers();

-- Enhance widgets table
SELECT topsie.rti_enhance_table('widgets', ARRAY['c1']);

-- Verify that selects query all shards
EXPLAIN (COSTS false) SELECT COUNT(*) FROM widgets;

-- Insert some data
INSERT INTO widgets VALUES ( 1, 'first row');
INSERT INTO widgets VALUES ( 2, 'second row');
INSERT INTO widgets VALUES ( 3, 'third row');
INSERT INTO widgets VALUES ( 4, 'fourth row');
INSERT INTO widgets VALUES ( 5, 'fifth row');
INSERT INTO widgets VALUES ( 6, 'sixth row');
INSERT INTO widgets VALUES ( 7, 'seventh row');
INSERT INTO widgets VALUES ( 8, 'eighth row');
INSERT INTO widgets VALUES ( 9, 'ninth row');
INSERT INTO widgets VALUES (10, 'last row');

-- Check that no duplicate data exists when querying all shards...
SELECT COUNT(DISTINCT c1) FROM widgets;

-- ... and that each shard has some data itself
SELECT COUNT(DISTINCT c1) FROM topsie.widgets_s0_r0;
SELECT COUNT(DISTINCT c1) FROM topsie.widgets_s1_r0;

-- Check that our table is now a view with a trigger
\dS widgets

-- Check for the foreign servers and tables.
SELECT COUNT(*) FROM pg_foreign_server;
SELECT COUNT(*) FROM pg_foreign_table;

DROP EXTENSION topsie; -- fail, because the trigger function lives in extension

DROP EXTENSION topsie CASCADE;
