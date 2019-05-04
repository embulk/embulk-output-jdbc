# PostgreSQL output plugin for Embulk

PostgreSQL output plugin for Embulk loads records to PostgreSQL.

## Overview

* **Plugin type**: output
* **Load all or nothing**: depends on the mode. see below.
* **Resume supported**: depends on the mode. see below.

## Configuration

- **driver_path**: path to the jar file of the PostgreSQL JDBC driver. If not set, the bundled JDBC driver (PostgreSQL JDBC Driver 9.4-1205) will be used. (string)
- **host**: database host name (string, required)
- **port**: database port number (integer, default: 5432)
- **user**: database login user name (string, required)
- **password**: database login password (string, default: "")
- **database**: destination database name (string, required)
- **schema**: destination schema name (string, default: "public")
- **temp_schema**: schema name for intermediate tables. by default, intermediate tables will be created in the schema specified by `schema`. replace mode doesn't support temp_schema. (string, optional)
- **table**: destination table name (string, required)
- **create_table_constraint**: table constraint added to `CREATE TABLE` statement, like `CREATE TABLE <table_name> (<column1> <type1>, <column2> <type2>, ..., <create_table_constraint>) <create_table_option>`.
- **create_table_option**: table option added to `CREATE TABLE` statement, like `CREATE TABLE <table_name> (<column1> <type1>, <column2> <type2>, ..., <create_table_constraint>) <create_table_option>`.
- **transaction_isolation**: transaction isolation level for each connection ("read_uncommitted", "read_committed", "repeatable_read" or "serializable"). if not specified, database default value will be used.
- **options**: extra connection properties (hash, default: {})
- **retry_limit**: max retry count for database operations (integer, default: 12). When intermediate table to create already created by another process, this plugin will retry with another table name to avoid collision.
- **retry_wait**: initial retry wait time in milliseconds (integer, default: 1000 (1 second))
- **max_retry_wait**: upper limit of retry wait, which will be doubled at every retry (integer, default: 1800000 (30 minutes))
- **mode**: "insert", "insert_direct", "truncate_insert", "replace", "merge" or "merge_direct". See below. (string, required)
- **merge_keys**: key column names for merging records in merge mode (string array, required in merge mode if table doesn't have primary key)
- **merge_rule**: list of column assignments for updating existing records used in merge mode, for example `foo = foo + S.foo` (`S` means source table). (string array, default: always overwrites with new values)
- **ssl**: enables SSL. data will be encrypted but CA or certification will not be verified (boolean, default: false)
- **batch_size**: size of a single batch insert (integer, default: 16777216)
- **default_timezone**: If input column type (embulk type) is timestamp, this plugin needs to format the timestamp into a SQL string. This default_timezone option is used to control the timezone. You can overwrite timezone for each columns using column_options option. (string, default: `UTC`)
- **column_options**: advanced: a key-value pairs where key is a column name and value is options for the column.
  - **type**: type of a column when this plugin creates new tables (e.g. `VARCHAR(255)`, `INTEGER NOT NULL UNIQUE`). This used when this plugin creates intermediate tables (insert, truncate_insert and merge modes), when it creates the target table (insert_direct and replace modes), and when it creates nonexistent target table automatically. (string, default: depends on input column type. `BIGINT` if input column type is long, `BOOLEAN` if boolean, `DOUBLE PRECISION` if double, `CLOB` if string, `TIMESTAMP WITH TIME ZONE` if timestamp)
  - **value_type**: This plugin converts input column type (embulk type) into a database type to build a INSERT statement. This value_type option controls the type of the value in a INSERT statement. (string, default: depends on the sql type of the column. Available values options are: `byte`, `short`, `int`, `long`, `double`, `float`, `boolean`, `string`, `nstring`, `date`, `time`, `timestamp`, `decimal`, `json`, `null`, `pass`)
  - **timestamp_format**: If input column type (embulk type) is timestamp and value_type is `string` or `nstring`, this plugin needs to format the timestamp value into a string. This timestamp_format option is used to control the format of the timestamp. (string, default: `%Y-%m-%d %H:%M:%S.%6N`)
  - **timezone**: If input column type (embulk type) is timestamp, this plugin needs to format the timestamp value into a SQL string. In this cases, this timezone option is used to control the timezone. (string, value of default_timezone option is used by default)
- **before_load**: if set, this SQL will be executed before loading all records. In truncate_insert mode, the SQL will be executed after truncating. replace mode doesn't support this option.
- **after_load**: if set, this SQL will be executed after loading all records.

### Modes

* **insert**:
  * Behavior: This mode writes rows to some intermediate tables first. If all those tasks run correctly, runs `INSERT INTO <target_table> SELECT * FROM <intermediate_table_1> UNION ALL SELECT * FROM <intermediate_table_2> UNION ALL ...` query. If the target table doesn't exist, it is created automatically.
  * Transactional: Yes. This mode successfully writes all rows, or fails with writing zero rows.
  * Resumable: Yes.
* **insert_direct**:
  * Behavior: This mode inserts rows to the target table directly. If the target table doesn't exist, it is created automatically.
  * Transactional: No. If fails, the target table could have some rows inserted.
  * Resumable: No.
* **truncate_insert**:
  * Behavior: Same with `insert` mode excepting that it truncates the target table right before the last `INSERT ...` query.
  * Transactional: Yes.
  * Resumable: Yes.
* **replace**:
  * Behavior: This mode writes rows to an intermediate table first. If all those tasks run correctly, drops the target table and alters the name of the intermediate table into the target table name.
  * Transactional: Yes.
  * Resumable: No.
* **merge**:
  * Behavior: This mode writes rows to some intermediate tables first. If all those tasks run correctly, runs `WITH updated AS (UPDATE .... RETURNING ...) INSERT INTO ....` query. Namely, if merge keys of a record in the intermediate tables already exist in the target table, the target record is updated by the intermediate record, otherwise the intermediate record is inserted. If the target table doesn't exist, it is created automatically.
  * Transactional: Yes.
  * Resumable: Yes.
* **merge_direct**:
  * Behavior: This mode inserts rows to the target table directly using `WITH S AS (SELECT ...), updated AS (UPDATE .... RETURNING ...) INSERT INTO ....` query. If the target table doesn't exist, it is created automatically.
  * Transactional: No.
  * Resumable: No.

### Supported types

|database type|default value_type|note|
|:--|:--|:--|
|bool|boolean||
|smallint|short||
|int|int||
|bigint|long||
|real|float||
|double precision|double||
|money|double||
|numeric|decimal||
|char|string||
|varchar|string||
|text|string||
|json|json||
|jsonb|json||
|date|date||
|time|time||
|timestamp|timestamp||

You can use other types by specifying `value_type` in `column_options`.

### Example

```yaml
out:
  type: postgresql
  host: localhost
  user: pg
  password: ""
  database: my_database
  table: my_table
  mode: insert
```

Advanced configuration:

```yaml
out:
  type: postgresql
  host: localhost
  user: pg
  password: ""
  database: my_database
  table: my_table
  options: {loglevel: 2}
  mode: insert_direct
  column_options:
    my_col_1: {type: 'BIGSERIAL'}
    my_col_3: {type: 'INT NOT NULL'}
    my_col_4: {value_type: string, timestamp_format: `%Y-%m-%d %H:%M:%S %z`, timezone: '-0700'}
    my_col_5: {type: 'DECIMAL(18,9)', value_type: pass}
```

### Build

```
$ ./gradlew gem
```
