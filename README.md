# pg_shard

pg\_shard is a sharding extension for PostgreSQL. The extension shards and replicates your PostgreSQL tables for horizontal scale and high availability. It also natively distributes SQL statements, without requiring any changes to your application logic.

As a standalone extension, pg\_shard addresses many NoSQL use-cases. In addition, you get SQL's expressiveness and PostgreSQL's powerful features, such as diverse indexes and semi-structured data types. Further, you can configure different replication set-ups using PostgreSQL's streaming replication, or easily upgrade your pg\_shard to CitusDB to boost your analytic query performance.

This Readme serves as a Quick Start Guide. We provide more details on sharding, shard rebalancing, and distributed consistency in here [XXX:link], and also go over some frequently asked questions. Also, we're actively working on improving pg\_shard, and welcome your feedback and questions in our mailing lists [XXX:link].

## Building

pg\_shard has been tested on most Linux and OS X platforms. The extension works with PostgreSQL 9.3 or 9.4, and CitusDB 3.2.

Once you have PostgreSQL or CitusDB installed, you will need to include the pg\_config directory path in your make command to build. This path is typically the same as your PostgreSQL installation's bin/ directory path. For example:

```
PATH=/usr/local/pgsql/bin/:$PATH make
sudo PATH=/usr/local/pgsql/bin/:$PATH make install
```

pg\_shard includes comprehensive regression tests. To verify your installation, start you Postgres instance with the `shared_preload_libraries` setting below, and run `make installcheck`.

## Setup

pg\_shard uses a master node to store the shard metadata and to act as the interface for all queries to the cluster. Before setup, pick one of your cluster nodes as the master node. The other nodes in the cluster will then be your worker nodes. The following instructions should only be applied on the master node for the simple set-up.

Before using pg\_shard you need to add it to `shared_preload_libraries` in your `postgresql.conf` and restart Postgres:

    shared_preload_libraries = 'pg_shard.so'    # (change requires restart)

The master node in pg\_shard reads worker host information from a file called `pg_worker_list.conf` in the data directory. You need to add the hostname and port number for each worker node in your cluster to this file. For example, to add two worker nodes running on the default PostgreSQL port:

    $ emacs -nw $PGDATA/pg_worker_list.conf

    # hostname port-number
    worker-101  5432
    worker-102  5432

Save and restart the master node.

Now, let's log into the master node and first create the extension:

```sql
CREATE EXTENSION pg_shard;
```

At this point you're ready to distribute a table. To let pg\_shard know the structure of your table, define its schema as you would do with a normal table:

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
-- Specify the table to distribute and the column to distribute it on 
SELECT master_create_distributed_table('customer_reviews', 'customer_id');
```

This function informs pg\_shard that the table `customer_reviews` should be hash partitioned on the `customer_id` column.

Now create shards for this table on the worker nodes:

```sql
-- Specify the table name, total shard count and replication factor
SELECT master_create_worker_shards('customer_reviews', 16, 2);
```

This function creates a total of 16 shards. Each shard owns a portion of a hash token space, and gets replicated on 2 worker nodes. The shard replicas created on the worker nodes have the same table schema, index, and constraint definitions as the table on the master node. Once all replicas are created, this function saves all distributed metadata on the master node.

## Usage

Once you created your shards, you can start issuing queries against the cluster. Currently, `SELECT`, `UPDATE` and
`DELETE` commands require the partition column in the `where` clause.


```sql
INSERT INTO customer_reviews (customer_id, review_rating) VALUES (4687, 5);
INSERT INTO customer_reviews (customer_id, review_rating, product_title) VALUES (4687, 5, 'Harry Potter');
INSERT INTO customer_reviews (customer_id, review_rating) VALUES (4700, 10);
```

```sql
SELECT count(*) FROM customer_reviews WHERE customer_id = 4687;
SELECT * FROM customer_reviews WHERE customer_id = 4687;
```

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
