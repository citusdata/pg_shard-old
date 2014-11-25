# pg_shard

pg\_shard is a sharding extension for PostgreSQL. It shards and replicates your PostgreSQL tables for horizontal scale and high availability. The extension also seamlessly distributes your SQL statements, without requiring any changes at the application layer.

As a standalone extension, pg\_shard addresses many NoSQL use cases. In addition, you get SQL's expressiveness and PostgreSQL's powerful features, such as diverse indexes and semi-structured data types. Further, you can easily upgrade your pg\_shard to CitusDB if you'd like to boost query performance for complex analytic queries.

This document serves as a quick start guide. We provide more details on sharding, shard rebalancing, different replication setups, and how pg\_shard achieves distributed consistency in our webpage [XXX:link]. Also, we're actively working on improving pg\_shard, and welcome your feedback and questions in our mailing lists [XXX:link].

## Building

pg\_shard has been tested on most Linux and OS X platforms. The extension works with PostgreSQL 9.3 or 9.4, and CitusDB 3.2.

Once you have PostgreSQL or CitusDB installed, you will need to include the pg\_config directory path in your make command to build. This path is typically the same as your PostgreSQL installation's bin/ directory path. For example:

```
PATH=/usr/local/pgsql/bin/:$PATH make
sudo PATH=/usr/local/pgsql/bin/:$PATH make install
```

pg\_shard includes comprehensive regression tests. To verify your installation, start you Postgres instance with the `shared_preload_libraries` setting below, and run `make installcheck`.

## Setup

pg\_shard uses a master node to store the shard metadata that also acts as the interface for all queries to the cluster. Before setup, pick one of your nodes as the master node. The other nodes in the cluster will then be your worker nodes.

To test this extension, you can just run your master and worker nodes on the same machine. In that case, each node will be one PostgreSQL database that runs on a different port. You can simply use ```localhost``` as the hostname in this case.

Or, you could start up one PostgreSQL database per machine; this is more applicable to production workloads. In this case, you'll need to configure your Postgres instances so that they can talk to each other. For that, you'll need to update the ```listen_addressess``` setting in your postgresql.conf file, and change access control settings in pg_hba.conf.

Once you decide on your cluster setup, you will need to make two changes on the master node. First, you will need to add pg\_shard to `shared_preload_libraries` in your `postgresql.conf`:

    shared_preload_libraries = 'pg_shard.so'    # (change requires restart)

Second, the master node in pg\_shard reads worker host information from a file called `pg_worker_list.conf` in the data directory. You need to add the hostname and port number for each worker node in your cluster to this file. For example, to add two worker nodes running on the default PostgreSQL port:

    $ emacs -nw $PGDATA/pg_worker_list.conf

    # hostname port-number
    worker-101  5432
    worker-102  5432

Save and restart the master node.

### Table Sharding

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

This function informs pg\_shard that the table `customer_reviews` should be hash partitioned on the `customer_id` column. Now, let's create shards for this table on the worker nodes:

```sql
-- Specify the table name, total shard count and replication factor
SELECT master_create_worker_shards('customer_reviews', 16, 2);
```

This function creates a total of 16 shards. Each shard owns a portion of a hash token space, and gets replicated on 2 worker nodes. The shard replicas created on the worker nodes have the same table schema, index, and constraint definitions as the definitions on the master node. Once all replicas are created, this function saves all distributed metadata on the master node.

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
SELECT avg(review_rating) FROM customer_reviews WHERE customer_id = 4687;
```

```sql
UPDATE customer_reviews SET review_votes = 10 WHERE customer_id = 4687;
DELETE FROM customer_reviews WHERE customer_id = 4700;
```

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
