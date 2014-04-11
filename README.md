# topsie

`topsie` is a sharding extension for PostgreSQL that supports executing DML statements in real-time. It enables clients to be completely agnostic to the fact that partitioning is occuring by encapsulating the logic to route data to the proper nodes.

## Features

`topsie` is intentionally limited in scope during its first release, but is fully functional within that scope. By installing this extension, you will be able to:

  * Horizontally scale write throughput
  * Distribute incoming data using the hashed value of one column
  * Replicate shards across two or more nodes
  * Propagate an existing table schema across a set of nodes
  * Detect node failures to preserve consistency
  * Query the cluster using simple `SELECT` statements
  * Limit queries to a subset of shards using equality predicates
  * Integrate with PostgreSQL 9.3 and CitusDB 3.0 or higher

Features not yet available but under consideration for future releases:

  * Eventual consistency
  * `UPDATE` and `DELETE` support
  * Range-based partitioning
  * Modification of schema of existing distributed tables
  * Identically partitioning sets of tables to ensure their data is colocated

## Requirements

At minimum, you'll need:

  * PostgreSQL 9.3 or later or CitusDB 3.0 or later
  * `gcc` or `clang`
  * `make`

You can find information on obtaining PostgreSQL on their [download page](http://www.postgresql.org/download/).

## Installing

Once you have PostgreSQL or CitusDB installed and have downloaded a copy of `topsie`, installing the extension is straightforward:

  1. Ensure `pg_config` is on your `PATH`
  2. `cd` to the root of your copy of `topsie`
  3. Run `make install`

`topsie` includes comprehensive regression tests. To verify your installation, just run `make installcheck`.

## Setup

`topsie` stores configuration data in some tables within the `topsie` schema. The most interesting one for users is `nodes`, which contains the list of known worker nodes within the cluster. To get started with `topsie` you'll need to add some rows. The schema is:

| Column     | Type      | Modifiers   |
| ---------- | --------- | ----------- |
| `hostname` | `text`    | `not null`  |
| `port`     | `integer` | `not null`  |

**Indexes**:
  `"nodes_pkey" PRIMARY KEY, btree (hostname, port)`

Assuming you have nodes named `bohr` and `einstein` running PostgreSQL on the default port:

```sql
INSERT INTO nodes VALUES ('bohr', 5432), ('einstein', 5432);
```

At this point you're ready to distribute a table. To let `topsie` know the structure of your table, define its schema as you would with a normal table:

```sql
CREATE TABLE observations (
	object_id integer             not null,
	quantity  text                not null,
	time      timestamp           not null,
	unit      text                not null,
	value     double precision    not null
);
```

This table will not be used to store any data on the master but rather serves as a _prototype_ of what an `observations` table should look like on worker nodes. After you're happy with your schema, tell `topsie` to distribute your table:

```sql
-- Pass table name, hash key column, shard count, and replication factor
SELECT topsie.create_distributed_table_using('observations', 'quantity', 16, 2);
```

This function does a number of things to set up your distributed table:

  1. `shard_count` shards are recorded in `topsie.shards`
  2. For each shard, `replication_factor` nodes are selected. On each node, a table is created whose structure is identical to the prototype table
  3. These shard placements are recorded in `topsie.placements`
  4. The prototype table is moved into the `topsie_prototypes` schema
  5. A `FOREIGN TABLE` is created in the `public` schema whose name and structure match the prototype table

To verify that this has occurred, you can peek into the `shards` and `placements` tables in the `topsie` schema:

```sql
SELECT * FROM topsie.shards;
```

| `id` | `prototype_id` |  `min_value` |  `max_value` |
| ---- | -------------- | ------------ | ------------ |
|    1 |          16409 |  -2147483648 |  -1610612737 |
|    2 |          16409 |  -1610612736 |  -1073741825 |
|    3 |          16409 |  -1073741824 |   -536870913 |
|    4 |          16409 |   -536870912 |           -1 |
|    5 |          16409 |            0 |    536870911 |
|    6 |          16409 |    536870912 |   1073741823 |
|    7 |          16409 |   1073741824 |   1610612735 |
|    8 |          16409 |   1610612736 |   2147483647 |

```sql
SELECT * FROM topsie.placements;
```

| `shard_id` | `hostname` | `port` |
| ---------  | ---------- | ------ |
|         1  |   einstein |   5432 |
|         1  |   bohr     |   5432 |
|         2  |   einstein |   5432 |
|         2  |   bohr     |   5432 |
|         3  |   bohr     |   5432 |
|         3  |   einstein |   5432 |
|         4  |   bohr     |   5432 |
|         4  |   einstein |   5432 |
|         5  |   bohr     |   5432 |
|         5  |   einstein |   5432 |
|         6  |   einstein |   5432 |
|         6  |   bohr     |   5432 |
|         7  |   einstein |   5432 |
|         7  |   bohr     |   5432 |

From this we see the hash space was divided into eight shards, each of which was placed on two nodes (in a random order).

## Usage

Issuing `INSERT` and `SELECT` commands against the foreign table will transparently route your requests to the correct shard. If a suitable `WHERE` clause is present, only relevant shards will be queried.

```sql
EXPLAIN INSERT INTO observations  VALUES (4687, 'download', '2014-04-11 00:25:54 -0600'::timestamp, 'KiB/s', 154.1987);

--                           QUERY PLAN
-- -----------------------------------------------------------------
--  Insert on observations shard 4 (cost=0.00..0.01 rows=1 width=0)
--    ->  Result  (cost=0.00..0.01 rows=1 width=0)

EXPLAIN SELECT * FROM remote_observing;

--                                 QUERY PLAN
-- -----------------------------------------------------------------------
--  Foreign Scan on observations  (cost=100.00..132.74 rows=758 width=84)

EXPLAIN SELECT * FROM remote_observing WHERE time='2014-04-11 00:25:54 -0600'::timestamp;

--                                QUERY PLAN
-- ----------------------------------------------------------------------------------
--  Foreign Scan on observations (shard_id 4)  (cost=100.00..119.56 rows=4 width=84)
```

## Troubleshooting

If a node is unreachable, queries to it will fail immediately. Application operators should alarm on errors to let them know when to inspect nodes. Further support is available by contacting Citus Data. Please include dumps of all tables in the `topsie` schema as well as the relevant foreign tables.

## Limitations

`topsie` provides a nice abstraction for sharding, but certain features will never be supported:

  * Distributed `JOIN`s — Upgrade to CitusDB to unlock this feature
  * Unique constraints on columns other than the partition key
  * Foreign key constraints

### Future

Given enough demand, these features may be included in future releases:

  * `UPDATE` and `DELETE` support
  * Eventual consistency
  * Retries to unhealthy nodes
  * Range partitioning
  * Schema modifications
  * Partitioning many tables identically based on a common column

If we've missed something, please open an issue.
