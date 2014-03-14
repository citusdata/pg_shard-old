# topsie

[![Build Status](http://img.shields.io/travis/jasonmp85/pg_travis_research/master.svg)][status]

Sharding extension for PostgreSQL supporting DML statements in real-time.

## Prerequisites

The `postgres_fdw` extension must be installed:

```sh
psql -c 'CREATE EXTENSION postgres_fdw' postgres
```

## Installation

Simple as this:

```sh
git clone https://github.com/citusdata/topsie.git
cd topsie
make install
```

## Setup

Assuming the following setup:

  1. A master node running locally accessible via a socket
  2. Two worker nodes running locally accessible via TCP ports 5433 and 5434
  3. Worker nodes configured to trust IPv4 connections from localhost

Also assuming `pgbench` has been used to create some simple tables on the workers:

```sh
pgbench -i -h localhost -p 5433 postgres
pgbench -i -h localhost -p 5434 postgres
```

Then the master can be configured like so:

```sql
CREATE EXTENSION topsie;
CREATE TABLE pgbench_accounts (
    aid integer NOT NULL,
    bid integer,
    abalance integer,
    filler character(84)
);
INSERT INTO topsie.dist_shards (shard_id, table_name) VALUES (0, 'pgbench_accounts');
INSERT INTO topsie.dist_shards (shard_id, table_name) VALUES (1, 'pgbench_accounts');
INSERT INTO topsie.dist_shard_placements VALUES ('localhost', 5433, 1);
INSERT INTO topsie.dist_shard_placements VALUES ('localhost', 5434, 2);
SELECT topsie.create_servers();
SELECT topsie.rti_enhance_table('pgbench_accounts', ARRAY['aid']);
```

## Use

Just interact with the main table as normal:

### Querying

On the master node:

```sql
SELECT COUNT(*) FROM pgbench_accounts;
```

|count |
|:----:|
|200000|

Behind the scenes the extension is querying every shard:

```sql
EXPLAIN (COSTS false) SELECT * FROM pgbench_accounts;
```
|                  QUERY PLAN                  |
|:---------------------------------------------|
|`Append`                                      |
|`  ->  Foreign Scan on pgbench_accounts_s0_r0`|
|`  ->  Foreign Scan on pgbench_accounts_s1_r0`|

The `_s1_r0` suffix means "shard one, replica zero".

### Inserting

Just insert as normal on the master node to the main table:

```sql
INSERT INTO pgbench_accounts (aid, bid, abalance) VALUES (23456789, 100, 500.00);
-- INSERT 0 1
```

If you want to dig into the nitty-gritty you can discover which shard contains
the row you inserted:


```sql
SELECT * FROM topsie.pgbench_accounts_s0_r0 WHERE aid=23456789;
```

|aid|bid|abalance|filler|
|---|---|--------|------|
|   |   |        |      |

```sql
SELECT * FROM topsie.pgbench_accounts_s1_r0 WHERE aid=23456789;
```

|aid     |bid|abalance|filler|
|:------:|:-:|-------:|:----:|
|23456789|100|500     |∅     |

So this must have ended up on shard 1.

[status]: https://travis-ci.org/jasonmp85/pg_travis_research
