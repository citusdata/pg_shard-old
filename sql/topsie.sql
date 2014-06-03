--
--  Test topsie extension
--

CREATE EXTENSION topsie;

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
