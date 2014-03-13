-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION topsie" to load this file. \quit

/*
 * Stores lists of shards for a given table. `shard_id` begins at 1 and should
 * not have any gaps within a given table.
 */
CREATE TABLE dist_shards (
  id serial primary key,
  table_name text not null,
  shard_id integer not null
);

-- register above as a config table to ensure it is included in dumps
SELECT pg_catalog.pg_extension_config_dump('dist_shards', '');

/*
 * Stores data about where given shards reside. Hostnames and ports are used to
 * connect to a particular server in order to fetch a given shard's data.
 */
CREATE TABLE dist_shard_placements (
  node_name text not null,
  node_port integer not null,
  dist_shard_id integer not null
);

-- register above as a config table to ensure it is included in dumps
SELECT pg_catalog.pg_extension_config_dump('dist_shard_placements', '');

/*
 * Helper function to build the system name for a given host/server pair.
 */
CREATE OR REPLACE FUNCTION fserver_name(hostname text, port integer) RETURNS text AS $fserver_name$
  BEGIN
    RETURN ('topsie_worker-' || md5(hostname || ':' || port));
  END;
$fserver_name$ LANGUAGE plpgsql;

/*
 * Helper function to build the system name for a particular replica of a shard
 * of a table.
 */
CREATE OR REPLACE FUNCTION ftable_name(table_name text, shard integer, replica integer) RETURNS text
AS $ftable_name$
  BEGIN
    RETURN format('%s_s%s_r%s', table_name, shard, replica);
  END;
$ftable_name$ LANGUAGE plpgsql;

/*
 * Returns a template which can be used to build a foreign table which mimics
 * the structure of the given relation, as far as that is possible using the
 * `postgres_fdw` extension. `NOT NULL` and `DEFAULT` specifiers will be used
 * if present on the specified relation, but other concerns (constraints, etc.)
 * cannot be specified on foreign tables and as such are not supported.
 *
 * The returned template is a format string suitable for use with `format` and
 * has three placeholders which must be specified:
 *
 *   1. a local name for the constructed foreign table
 *   2. the local name of the server to be used with this table
 *   3. the name of the remote table on the specified server
 *
 * After `format` has inserted these values, `EXECUTE` the output to actually
 * create the foreign table.
 */
CREATE OR REPLACE FUNCTION reflect_foreign_table_template(a_table regclass) RETURNS text
AS $reflect_foreign_table_template$
  DECLARE
    refl_query CONSTANT text := $refl_query$
      SELECT
          a.attname AS name,
          format_type(a.atttypid, atttypmod) AS type,
          pg_get_expr(d.adbin, d.adrelid) AS default_val,
          a.attnotnull AS notnull
      FROM pg_attribute a
      LEFT OUTER JOIN pg_attrdef d ON (d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef)
      WHERE
          a.attrelid = $1 AND
          a.attnum > 0
          AND NOT a.attisdropped
      ORDER BY a.attnum;
    $refl_query$;
    ret_val text := 'CREATE FOREIGN TABLE topsie.%I ( ';
    attr RECORD;
  BEGIN
    FOR attr IN EXECUTE refl_query USING a_table LOOP
      ret_val := ret_val || attr.name || ' ' || attr.type;

      IF attr.default_val IS NOT NULL THEN
        ret_val := ret_val || ' DEFAULT ' || attr.default_val;
      END IF;

      IF attr.notnull THEN
        ret_val := ret_val || ' NOT NULL';
      END IF;

      ret_val := ret_val || ',';
    END LOOP;

    ret_val := rtrim(ret_val, ',') || $$ ) SERVER %I OPTIONS ( schema_name 'public', table_name %L )$$;

    RETURN ret_val;
  END;
$reflect_foreign_table_template$ LANGUAGE plpgsql;

/*
 * Reads the configuration in `dist_shard_placements` and creates the foreign
 * server objects needed to connect to each unique hostname/port pair listed in
 * that table.
 *
 * Must be called as part of the setup of a new cluster before any call to
 * `rti_enhance_table` will succeed.
 */
CREATE OR REPLACE FUNCTION create_servers() RETURNS void AS $create_servers$
  DECLARE
    server_tmpl CONSTANT text := $$CREATE SERVER %I TYPE 'postgres_fdw'
      FOREIGN DATA WRAPPER postgres_fdw
      OPTIONS (
        host %L,
        port %L,
        dbname %L,
        application_name 'topsie'
      )$$;
    user_tmpl CONSTANT text := 'CREATE USER MAPPING FOR PUBLIC SERVER %I OPTIONS (user %L)';

    entry topsie.dist_shard_placements%ROWTYPE;
    server_name text;
  BEGIN
    FOR entry IN SELECT DISTINCT ON (node_name, node_port) * FROM topsie.dist_shard_placements LOOP
      server_name := topsie.fserver_name(entry.node_name, entry.node_port);

      EXECUTE format(server_tmpl, server_name, entry.node_name, entry.node_port, current_database());
      EXECUTE format(user_tmpl, server_name, CURRENT_USER);
    END LOOP;
  END;
$create_servers$ LANGUAGE plpgsql;

/*
 * Produces a 32-bit number to be used as a hash of the specified input. The
 * hash algorithm is as follows:
 *
 *   1. Elements of the input array are converted to UTF-8 and concatenated
 *   2. An md5 hash of this concatenated string is calculated
 *   3. The md5 hash is broken into four 32-bit numbers
 *   4. These numbers are XORed together and the result is returned
 */
CREATE OR REPLACE FUNCTION hash_values(input text[]) RETURNS integer AS $hash_values$
  DECLARE
    key text := convert_to('', 'utf-8');
    md5_regex CONSTANT text := '^(\w{8})(\w{8})(\w{8})(\w{8})$';
    hash integer := 0;

    item text;
    part text;
  BEGIN
    FOREACH item IN ARRAY input
    LOOP
        key := key || convert_to(item, 'utf-8');
    END LOOP;

    FOREACH part IN ARRAY regexp_matches(md5(key), md5_regex)
    LOOP
        hash := hash # ('x' || lpad(part, 8, '0'))::bit(32)::int;
    END LOOP;

    RETURN hash;
  END;
$hash_values$ LANGUAGE plpgsql;

/*
 * Helper function to return a dynamically-named column from a particular row.
 * There isn't a better way to do this in PL/pgSQL. So it goes.
 */
CREATE OR REPLACE FUNCTION select_text_values(a_record anyelement, col_names text[]) RETURNS text[] AS $select_text_values$
  DECLARE
    select_list CONSTANT text := rtrim(repeat('($1).%I::text,', array_length(col_names, 1)), ',');
    query_tmpl CONSTANT text := 'SELECT ARRAY[' || select_list || ']';
    col_values text[];
  BEGIN
    EXECUTE format(query_tmpl, VARIADIC col_names) INTO STRICT col_values USING a_record;

    RETURN col_values;
  END;
$select_text_values$ LANGUAGE plpgsql;

/*
 * Trigger to execute upon an insert to an rti-enhanced table. Takes three
 * arguments at trigger creation time:
 *
 *   1. The total shard count for the given table
 *   2. The replication count for the cluster
 *   3. A comma-separated string of column names specifying the partition key
 *
 * During execution, this function finds the shard for the incoming row and
 * performs inserts to all replicas of that shard.
 */
CREATE OR REPLACE FUNCTION t001_shard_insert() RETURNS trigger AS $shard_insert$
  DECLARE
    shard_count   CONSTANT integer := TG_ARGV[0]::integer;
    replica_count CONSTANT integer := TG_ARGV[1]::integer;
    hash_cols     CONSTANT text[]  := string_to_array(TG_ARGV[2], ',');
    insert_tmpl   CONSTANT text    := 'INSERT INTO topsie.%I VALUES ($1.*)';
    shard_id      CONSTANT integer := @ (topsie.hash_values(topsie.select_text_values(NEW, hash_cols)) % shard_count);
  BEGIN
    FOR replica_id IN 0..(replica_count - 1) LOOP
        EXECUTE format(insert_tmpl, topsie.ftable_name(TG_TABLE_NAME, shard_id, replica_id)) USING NEW;
    END LOOP;

    RETURN NEW;
  END;
$shard_insert$ LANGUAGE plpgsql;

/*
 * topsie's main user-facing function. When passed a table name and list of
 * columns, "enhances" that table to permit real-time inserts. In addition, the
 * table is transformed to a view to allow queries across all shards.
 *
 * TODO: Determine replica count automatically (hardcoded to 0)
 * TODO: Determine hash_cols automatically (currently a parameter)
 * TODO: Add back DELETE/UPDATE support
 * TODO: Directly call create_servers() for user
 */
CREATE OR REPLACE FUNCTION rti_enhance_table(a_table regclass, hash_cols text[]) RETURNS void AS $rti_enhance_table$
    DECLARE
        i_trg_tmpl CONSTANT text := $$CREATE TRIGGER dist_insert INSTEAD OF INSERT ON %I
            FOR EACH ROW EXECUTE PROCEDURE topsie.t001_shard_insert(%L, %L, %L)$$;
        ftable_tmpl CONSTANT text    := topsie.reflect_foreign_table_template(a_table);
        shard_count   integer;
        rule_tmpl text := 'CREATE OR REPLACE RULE "_RETURN" AS ON SELECT TO %I DO INSTEAD ';
        placement_info RECORD;
        shard_id integer;

        table_name text;
        server_name text;
    BEGIN
        SELECT max(ds.shard_id) + 1 INTO STRICT shard_count FROM topsie.dist_shards ds WHERE ds.table_name=a_table::text;

        FOR placement_info IN
        SELECT node_name, node_port, ds.shard_id, (row_number() over(PARTITION BY ds.shard_id)) - 1 AS replica_id
        FROM topsie.dist_shards ds, topsie.dist_shard_placements dsp
        WHERE ds.id=dsp.dist_shard_id AND ds.table_name=a_table::text ORDER BY shard_id LOOP
            table_name := topsie.ftable_name(a_table::text, placement_info.shard_id, placement_info.replica_id::integer);
            server_name := topsie.fserver_name(placement_info.node_name, placement_info.node_port);

            EXECUTE format(ftable_tmpl, table_name, server_name, a_table || '_s' || placement_info.shard_id);
        END LOOP;

        FOR shard_id IN 0..(shard_count - 1) LOOP
            rule_tmpl := rule_tmpl || format('SELECT * FROM topsie.%I UNION ALL ', topsie.ftable_name(a_table::text, shard_id, 0));
        END LOOP;

        rule_tmpl := left(rule_tmpl, -11);
        EXECUTE format(rule_tmpl, a_table);

        EXECUTE format(i_trg_tmpl, a_table, shard_count, 1, array_to_string(hash_cols, ','));
    END;
$rti_enhance_table$ LANGUAGE plpgsql;
