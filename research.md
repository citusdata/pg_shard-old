# Manual Prototype Setup

This documents the steps I took to enable real-time inserts by hand.

## Background

Create a standard master `postgres` instance and a node instance identical to it running on port `5433`.

    pg_init master
    pg_init node
    # Update node to use port 5433

Run `pgbench -i` against the node instance in the default (`postgres`) database.

    pgbench -i -h localhost -p5433 postgres

Create the `postgres_fdw` extension on the master instance:

    CREATE EXTENSION postgres_fdw;

## Connecting `postgres_fdw`

On the master node, create a foreign server pointing at the node:

    CREATE SERVER node_server TYPE 'postgres_fdw'
        FOREIGN DATA WRAPPER postgres_fdw
        OPTIONS (
          host 'localhost',
          dbname 'postgres',
          port '5433',
          application_name 'topsie'
        );

Next you'll need a user mapping:

    CREATE USER MAPPING FOR jason
        SERVER node_server
        OPTIONS ( user 'jason' );

Finally, create the foreign tables:

    CREATE FOREIGN TABLE pgbench_accounts (
        aid integer NOT NULL,
        bid integer,
        abalance integer,
        filler character(84)
    ) SERVER node_server;

    CREATE FOREIGN TABLE pgbench_branches (
        bid integer NOT NULL,
        bbalance integer,
        filler character(88)
    ) SERVER node_server;

    CREATE FOREIGN TABLE pgbench_history (
        tid integer,
        bid integer,
        aid integer,
        delta integer,
        mtime timestamp without time zone,
        filler character(22)
    ) SERVER node_server;

    CREATE FOREIGN TABLE pgbench_tellers (
        tid integer NOT NULL,
        bid integer,
        tbalance integer,
        filler character(84)
    ) SERVER node_server;

`ANALYZE` must be told to analyze foreign tables:

    ANALYZE pgbench_accounts;
    ANALYZE pgbench_branches;
    ANALYZE pgbench_history;
    ANALYZE pgbench_tellers;

## Quick Benchmarks

So what does this setup look like from a performance standpoint? We can compare by running `pgbench` directly against the node, and then against the master:

    pgbench -h localhost -p5433 -n -T180 -r postgres

The `-n` flag is required (at least when connecting to the master) because it can't `VACUUM` foreign tables. On my laptop, hitting the node instance directly yields:

    transaction type: TPC-B (sort of)
    scaling factor: 1
    query mode: simple
    number of clients: 1
    number of threads: 1
    duration: 180 s
    number of transactions actually processed: 263614
    tps = 1464.569235 (including connections establishing)
    tps = 1464.586273 (excluding connections establishing)
    statement latencies in milliseconds:
        0.001882    \set nbranches 1 * :scale
        0.000587    \set ntellers 10 * :scale
        0.000559    \set naccounts 100000 * :scale
        0.000657    \setrandom aid 1 :naccounts
        0.000435    \setrandom bid 1 :nbranches
        0.000477    \setrandom tid 1 :ntellers
        0.000529    \setrandom delta -5000 5000
        0.036113    BEGIN;
        0.109400    UPDATE pgbench_accounts SET abalance = abalance + :delta WHERE aid = :aid;
        0.090026    SELECT abalance FROM pgbench_accounts WHERE aid = :aid;
        0.096513    UPDATE pgbench_tellers SET tbalance = tbalance + :delta WHERE tid = :tid;
        0.090787    UPDATE pgbench_branches SET bbalance = bbalance + :delta WHERE bid = :bid;
        0.075227    INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) VALUES (:tid, :bid, :aid, :delta, CURRENT_TIMESTAMP);
        0.173568    END;

And here are the results running against the master (after reinitializing the `pgbench` tables on the node):

    transaction type: TPC-B (sort of)
    scaling factor: 1
    query mode: simple
    number of clients: 1
    number of threads: 1
    duration: 180 s
    number of transactions actually processed: 71348
    tps = 396.386221 (including connections establishing)
    tps = 396.390498 (excluding connections establishing)
    statement latencies in milliseconds:
        0.002113    \set nbranches 1 * :scale
        0.000660    \set ntellers 10 * :scale
        0.000601    \set naccounts 100000 * :scale
        0.000752    \setrandom aid 1 :naccounts
        0.000501    \setrandom bid 1 :nbranches
        0.000554    \setrandom tid 1 :ntellers
        0.000569    \setrandom delta -5000 5000
        0.038898    BEGIN;
        0.614532    UPDATE pgbench_accounts SET abalance = abalance + :delta WHERE aid = :aid;
        0.296174    SELECT abalance FROM pgbench_accounts WHERE aid = :aid;
        0.524356    UPDATE pgbench_tellers SET tbalance = tbalance + :delta WHERE tid = :tid;
        0.513729    UPDATE pgbench_branches SET bbalance = bbalance + :delta WHERE bid = :bid;
        0.275833    INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) VALUES (:tid, :bid, :aid, :delta, CURRENT_TIMESTAMP);
        0.246877    END;

So using the `postgres_fdw`-wrapped tables we have a 72% decrease in transactions processed. We're mostly interested in the `INSERT` time but it also exhibits the 72% decrease, so that's what we're looking at. Nothing to write home about, but the benefit is in being able to route rows to the right shard transparently. Performance can be added later.
