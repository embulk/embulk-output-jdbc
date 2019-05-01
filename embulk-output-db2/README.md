# DB2 output plugins for Embulk

DB2 output plugins for Embulk loads records to DB2.

## Overview

* **Plugin type**: output
* **Load all or nothing**: depends on the mode. see below.
* **Resume supported**: depends on the mode. see below.

## Configuration

- **driver_path**: path to the jar file of the DB2 JDBC driver (string)
- **host**: database host name (string, required)
- **port**: database port number (integer, default: 50000)
- **user**: database login user name (string, required)
- **password**: database login password (string)
- **database**: destination database name (string, required)
- **schema**: destination schema name (string, default: use the default schema)
- **table**: destination table name (string, required)
- **create_table_constraint**: table constraint added to `CREATE TABLE` statement, like `CREATE TABLE <table_name> (<column1> <type1>, <column2> <type2>, ..., <create_table_constraint>) <create_table_option>`.
- **create_table_option**: table option added to `CREATE TABLE` statement, like `CREATE TABLE <table_name> (<column1> <type1>, <column2> <type2>, ..., <create_table_constraint>) <create_table_option>`.
- **transaction_isolation**: transaction isolation level for each connection ("read_uncommitted", "read_committed", "repeatable_read" or "serializable"). if not specified, database default value will be used.
- **options**: extra connection properties (hash, default: {})
- **retry_limit**: max retry count for database operations (integer, default: 12)
- **retry_wait**: initial retry wait time in milliseconds (integer, default: 1000 (1 second))
- **max_retry_wait**: upper limit of retry wait, which will be doubled at every retry (integer, default: 1800000 (30 minutes))
- **mode**: "insert", "insert_direct", "truncate_insert" or "replace". See below. (string, required)
- **insert_method**: see below
- **batch_size**: size of a single batch insert (integer, default: 16777216)
- **default_timezone**: If input column type (embulk type) is timestamp, this plugin needs to format the timestamp into a SQL string. This default_timezone option is used to control the timezone. You can overwrite timezone for each columns using column_options option. (string, default: `UTC`)
- **column_options**: advanced: a key-value pairs where key is a column name and value is options for the column.
  - **type**: type of a column when this plugin creates new tables (e.g. `VARCHAR(255)`, `INTEGER NOT NULL UNIQUE`). This used when this plugin creates intermediate tables (insert, insert_truncate and merge modes), when it creates the target table (insert_direct, merge_direct and replace modes), and when it creates nonexistent target table automatically. (string, default: depends on input column type. `BIGINT` if input column type is long, `SMALLINT` if boolean, `DOUBLE` if double, `CLOB` if string, `TIMESTAMP` if timestamp)
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
  * Transactional: No. If fails, the target table could be dropped (because DB2 can't rollback DDL).
  * Resumable: No.

### Supported types

|database type|default value_type|note|
|:--|:--|:--|
|SMALLINT|short||
|INTEGER|int||
|BITINT|long||
|DECIMAL|decimal||
|NUMERIC|decimal||
|REAL|float||
|DOUBLE|double||
|FLOAT|double||
|CHAR|string||
|VARCHAR|string||
|CLOB|string||
|GRAPHIC|string||
|VARGRAPHIC|string||
|NCHAR|string||
|NVARCHAR|string||
|NCLOB|string||
|DATE|date||
|TIME|time||
|TIMESTAMP|timestamp||

You can use other types by specifying `value_type` in `column_options`.

### Example

```yaml
out:
  type: db2
  driver_path: /opt/db2/db2jcc4.jar
  host: localhost
  user: myuser
  password: ""
  database: my_database
  table: my_table
  mode: insert
```

Advanced configuration:

```yaml
out:
  type: db2
  driver_path: /opt/db2/db2jcc4.jar
  host: localhost
  user: myuser
  password: ""
  database: my_database
  table: my_table
  mode: insert_direct
  insert_method: native
  column_options:
    my_col_1: {type: 'TEXT'}
    my_col_3: {type: 'INT NOT NULL'}
    my_col_4: {value_type: string, timestamp_format: `%Y-%m-%d %H:%M:%S %z`, timezone: '-0700'}
    my_col_5: {type: 'DECIMAL(18,9)', value_type: pass}
```

### Build

```
$ ./gradlew gem
```
