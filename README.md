# pg_shard

`pg_shard` is a sharding extension for PostgreSQL that supports executing DML statements in real-time. It enables clients to be completely agnostic to the fact that partitioning is occuring by encapsulating the logic to route data to the proper nodes.

## Features

`pg_shard` is intentionally limited in scope during its first release, but is fully functional within that scope. By installing this extension, you will be able to:

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

Once you have PostgreSQL or CitusDB installed and have downloaded a copy of `pg_shard`, installing the extension is straightforward:

`1. PATH=/usr/local/pgsql/bin/:$PATH make`

`2. sudo PATH=/usr/local/pgsql/bin/:$PATH make install`

`pg_shard` includes comprehensive regression tests. To verify your installation, just run `make installcheck`.

## Setup

`pg_shard` uses a master node to store the metadata and act as the interface
for all queries to the cluster. Before setup, pick one of your cluster nodes as
the master. The rest of the nodes then are your workers. The instructions below
should be applied only on the master node.

Before using pg_shard you need to add it to `shared_preload_libraries` in your
`postgresql.conf` and restart Postgres:

    shared_preload_libraries = 'pg_shard.so'    # (change requires restart)

The master node in `pg_shard` reads worker host information from a file called
`pg_worker_list.conf` in the data directory. We need to add the hostname and
port number information for each worker node in our cluster. The below example
adds two example worker nodes running on port 5432

    $ emacs -nw $PGDATA/pg_worker_list.conf

    # hostname port-number
    worker-101  5432
    worker-102  5432

Save and restart the master node.

Now, let's log into the master node and first create the extension:

```sql
CREATE EXTENSION pg_shard;
```

At this point you're ready to distribute a table. To let `pg_shard` know the structure of your table, define its schema as you would with a normal table:

```sql
CREATE TABLE customer_reviews
(
    customer_id INTEGER,
    review_date DATE,
    review_rating INTEGER,
    review_votes INTEGER,
    review_helpful_votes INTEGER,
    product_id CHAR(10),
    product_title TEXT,
    product_sales_rank BIGINT,
    product_group TEXT,
    product_category TEXT,
    product_subcategory TEXT,
    similar_product_ids CHAR(10)[]
);
```

This table will not be used to store any data on the master but rather serves as a _prototype_ of what a `customer_reviews` table should look like on worker nodes. After you're happy with your schema, tell `pg_shard` to distribute your table:

```sql
-- Pass table name and the column name on which you want to distribute your data
SELECT create_distributed_table('customer_reviews', 'customer_id');
```

This function informs `pg_shard` that the given table is to be hash partitioned
the `customer_id` column.

Now create shards for this table on the worker nodes:

```sql
-- Pass the table name, desired shard count and the replication factor
SELECT create_shards('customer_reviews', 16, 2);
```

This function does a number of things to set up your distributed table:

  1. `shard_count` shards are recorded in `pgs_distribution_metadata.shard`
  2. For each shard, `replication_factor` nodes are selected. On each node, a table is created whose structure is identical to the prototype table
  3. These shard placements are recorded in `pgs_distribution_metadata.shard_placement`

## Usage

Issuing `INSERT` and `SELECT` commands against the table will transparently
route your requests to the correct shard. Currently, `SELECT`, `UPDATE` and
`DELETE` commands require the partition column in the where-clause.


```sql
INSERT INTO customer_reviews (customer_id, review_rating) VALUES (4687, 5);
INSERT INTO customer_reviews (customer_id, review_rating, product_title) VALUES (4687, 5, 'Harry Potter');
INSERT INTO customer_reviews (customer_id, review_rating) VALUES (4700, 10);
```

```sql
SELECT count(*) FROM customer_reviews WHERE customer_id = 4687;
SELECT * FROM customer_reviews WHERE customer_id = 4687;
```

You can also issue `UPDATE` or `DELETE` commands on the distributed table.
```sql
UPDATE customer_reviews SET review_votes = 10 WHERE customer_id = 4687;
DELETE FROM customer_reviews WHERE customer_id = 4700;
```

## Troubleshooting

If a node is unreachable, queries to it will fail immediately. Application operators should alarm on errors to let them know when to inspect nodes. Further support is available by contacting Citus Data. Please include dumps of all tables in the `pg_shard` schema as well as the relevant foreign tables.

## Limitations

`pg_shard` provides a nice abstraction for sharding, but certain features will never be supported:

  * Distributed `JOIN`s — Upgrade to CitusDB to unlock this feature
  * Unique constraints on columns other than the partition key
  * Foreign key constraints

### Future

Given enough demand, these features may be included in future releases:

  * Eventual consistency
  * Marking nodes as unhealthy after a number of failures
  * Retries to unhealthy nodes
  * Partitioning using more than one column
  * Range partitioning
  * Schema modifications
  * Partitioning many tables identically based on a common column

If we've missed something, please open an issue.
