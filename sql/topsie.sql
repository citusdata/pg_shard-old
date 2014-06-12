-- ===================================================================
-- create FDW objects
-- ===================================================================

CREATE EXTENSION topsie;

CREATE SERVER testserver1 FOREIGN DATA WRAPPER topsie;
CREATE SERVER loopback FOREIGN DATA WRAPPER topsie
  OPTIONS (dbname 'contrib_regression');

CREATE USER MAPPING FOR public SERVER testserver1
	OPTIONS (user 'value', password 'value');
CREATE USER MAPPING FOR CURRENT_USER SERVER loopback;

-- ===================================================================
-- create tables to be used as shards 
-- ===================================================================
CREATE SCHEMA "topsie_tests";

CREATE TABLE "topsie_tests"."events_1" (
	id int primary key,
	name text not null,
	quantity float not null,
	moment timestamp not null
);

CREATE TABLE "topsie_tests"."events_2" (
	LIKE "topsie_tests"."events_1"
);

-- ===================================================================
-- create foreign table to write into shards
-- ===================================================================
CREATE FOREIGN TABLE ft1 (
	id int not null,
	name text not null,
	quantity float not null,
	moment timestamp not null
) SERVER loopback
OPTIONS (partition_column 'id');

-- ===================================================================
-- add mappings for the shards
-- ===================================================================

INSERT INTO topsie_metadata.shards (relation_id, min_value, max_value)
VALUES
	('ft1'::regclass, -2147483648, -1),
	('ft1'::regclass, 0, 2147483647);

--- empty string results in connection to localhost
INSERT INTO topsie_metadata.placements (shard_id, host, port)
SELECT shards.id, '', 5432 FROM topsie_metadata.shards;

-- ===================================================================
-- tests for validator
-- ===================================================================
-- requiressl, krbsrvname and gsslib are omitted because they depend on
-- configure options
ALTER SERVER testserver1 OPTIONS (
	use_remote_estimate 'false',
	updatable 'true',
	fdw_startup_cost '123.456',
	fdw_tuple_cost '0.123',
	service 'value',
	connect_timeout 'value',
	dbname 'value',
	--client_encoding 'value',
	application_name 'value',
	--fallback_application_name 'value',
	keepalives 'value',
	keepalives_idle 'value',
	keepalives_interval 'value',
	-- requiressl 'value',
	sslcompression 'value',
	sslmode 'value',
	sslcert 'value',
	sslkey 'value',
	sslrootcert 'value',
	sslcrl 'value'
	--requirepeer 'value',
	-- krbsrvname 'value',
	-- gsslib 'value',
	--replication 'value'
);
ALTER USER MAPPING FOR public SERVER testserver1
	OPTIONS (DROP user, DROP password);
ALTER FOREIGN TABLE ft1 OPTIONS (schema_name 'topsie_tests', table_name 'events');
\det+

-- ===================================================================
-- test writable foreign table stuff
-- ===================================================================
INSERT INTO ft1 (id, name, quantity, moment)
VALUES (1, 'foo', 100, '2014-04-15 17:46:52.97132-06'::timestamp);

SELECT COUNT(*) FROM "topsie_tests"."events_1";
SELECT COUNT(*) FROM "topsie_tests"."events_2";

INSERT INTO ft1 (id, name, quantity, moment)
VALUES (2, 'bar', 100, '2014-04-15 17:47:52.97132-06'::timestamp);

SELECT COUNT(*) FROM "topsie_tests"."events_1";
SELECT COUNT(*) FROM "topsie_tests"."events_2";

-- Test multiple rows at once
INSERT INTO ft1 (id, name, quantity, moment)
VALUES (3, 'foo', 100, '2014-04-15 17:47:52.97132-06'::timestamp),
	   (4, 'bar', 200, '2014-04-15 17:47:52.97132-06'::timestamp),
	   (5, 'baz', 300, '2014-04-15 17:47:52.97132-06'::timestamp),
	   (6, 'wat', 400, '2014-04-15 17:47:52.97132-06'::timestamp),
	   (7, 'how', 500, '2014-04-15 17:47:52.97132-06'::timestamp);

SELECT COUNT(*) FROM "topsie_tests"."events_1";
SELECT COUNT(*) FROM "topsie_tests"."events_2";

-- Test RETURNING
INSERT INTO ft1 (id, name, quantity, moment)
VALUES (8, 'foo', 100, '2014-04-15 17:47:52.97132-06'::timestamp)
RETURNING (id, name);

-- Test errors thrown on remote side
INSERT INTO ft1(id, name, quantity, moment)
VALUES (1, 'foo', 100, '2014-04-15 17:46:52.97132-06'::timestamp);

SELECT COUNT(*) FROM "topsie_tests"."events_1";
SELECT COUNT(*) FROM "topsie_tests"."events_2";

-- Test atomicity by having last row in a batch fail
INSERT INTO ft1 (id, name, quantity, moment)
VALUES (9, 'foo', 100, '2014-04-15 17:47:52.97132-06'::timestamp),
	   (10, 'bar', 200, '2014-04-15 17:47:52.97132-06'::timestamp),
	   (11, 'baz', 300, '2014-04-15 17:47:52.97132-06'::timestamp),
	   (12, 'wat', 400, '2014-04-15 17:47:52.97132-06'::timestamp),
	   (13, 'how', NULL, '2014-04-15 17:47:52.97132-06'::timestamp);

-- Counts should match what they were before last batch
SELECT COUNT(*) FROM "topsie_tests"."events_1";
SELECT COUNT(*) FROM "topsie_tests"."events_2";

--
--  Table with types commonly used in sharding.
--

CREATE TABLE many_types (
  one   bigint,
  two   date,
  three float8,
  four  integer,
  five  numeric,
  six   text,
  seven time,
  eight timestamp,
  nine  uuid
);

INSERT INTO many_types (
  one,
  two,
  three,
  four,
  five,
  six,
  seven,
  eight,
  nine
) VALUES (
  314159265358::bigint,
  '2008-05-03'::date,
  2.99792458::float8,
  123456789::integer,
  2.5::numeric,
  'A long time ago'::text,
  '03:15.50-07'::time,
  '2014-04-15 17:46:52.97132-06'::timestamp,
  'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid
);

--
--  Ensure hash values are what we expect.
--

SELECT topsie_hash(many_types, 1::smallint) FROM many_types;
SELECT topsie_hash(many_types, 2::smallint) FROM many_types;
SELECT topsie_hash(many_types, 3::smallint) FROM many_types;
SELECT topsie_hash(many_types, 4::smallint) FROM many_types;
SELECT topsie_hash(many_types, 5::smallint) FROM many_types;
SELECT topsie_hash(many_types, 6::smallint) FROM many_types;
SELECT topsie_hash(many_types, 7::smallint) FROM many_types;
SELECT topsie_hash(many_types, 8::smallint) FROM many_types;
SELECT topsie_hash(many_types, 9::smallint) FROM many_types;

--
--  TIME ZONE should not affect hashing
--

SET TIME ZONE 'CST7CDT';
SELECT topsie_hash(many_types, 7::smallint) FROM many_types;
SELECT topsie_hash(many_types, 8::smallint) FROM many_types;
RESET TIME ZONE;

--
--  NULL values should still hash
--

INSERT INTO many_types ( four ) VALUES ( 0 );
SELECT topsie_hash(many_types, 1::smallint) FROM many_types WHERE four = 0;

-- ===================================================================
-- test metadata functionality
-- ===================================================================

CREATE TABLE events ();

INSERT INTO topsie_metadata.shards (relation_id, min_value, max_value) VALUES
('events'::regclass, 0, 10),
('events'::regclass, 10, 20),
('events'::regclass, 20, 30),
('events'::regclass, 30, 40);

INSERT INTO topsie_metadata.placements (host, port, shard_id) VALUES
('foo', 123, 3),
('foo', 123, 4),
('bar', 456, 5),
('bar', 456, 6);

SELECT topsie_print_metadata('events'::regclass);
