# MySQL output plugins for Embulk

MySQL output plugins for Embulk loads records to MySQL.

## Overview

* **Plugin type**: output
* **Load all or nothing**: depnds on the mode. see below.
* **Resume supported**: depnds on the mode. see below.

## Configuration

- **host**: database host name (string, required)
- **port**: database port number (integer, default: 3306)
- **user**: database login user name (string, required)
- **password**: database login password (string, default: "")
- **database**: destination database name (string, required)
- **table**: destination table name (string, required)
- **options**: extra connection properties (hash, default: {})
- **mode**: "insert", "insert_direct", "truncate_insert", "merge", "merge_direct", or "replace". See below. (string, required)
- **batch_size**: size of a single batch insert (integer, default: 16777216)
- **default_timezone**: If input column type (embulk type) is timestamp, this plugin needs to format the timestamp into a SQL string. This default_timezone option is used to control the timezone. You can overwrite timezone for each columns using column_options option. (string, default: `UTC`)
- **column_options**: advanced: a key-value pairs where key is a column name and value is options for the column.
  - **type**: type of a column when this plugin creates new tables (e.g. `VARCHAR(255)`, `INTEGER NOT NULL UNIQUE`). This used when this plugin creates intermediate tables (insert, insert_truncate and merge modes), when it creates the target table (insert_direct, merge_direct and replace modes), and when it creates nonexistent target table automatically. (string, default: depends on input column type. `BIGINT` if input column type is long, `BOOLEAN` if boolean, `DOUBLE PRECISION` if double, `CLOB` if string, `TIMESTAMP` if timestamp)
  - **value_type**: This plugin converts input column type (embulk type) into a database type to build a INSERT statement. This value_type option controls the type of the value in a INSERT statement. (string, default: depends on the sql type of the column. Available values options are: `byte`, `short`, `int`, `long`, `double`, `float`, `boolean`, `string`, `nstring`, `date`, `time`, `timestamp`, `decimal`, `json`, `null`, `pass`)
  - **timestamp_format**: If input column type (embulk type) is timestamp and value_type is `string` or `nstring`, this plugin needs to format the timestamp value into a string. This timestamp_format option is used to control the format of the timestamp. (string, default: `%Y-%m-%d %H:%M:%S.%6N`)
  - **timezone**: If input column type (embulk type) is timestamp, this plugin needs to format the timestamp value into a SQL string. In this cases, this timezone option is used to control the timezone. (string, value of default_timezone option is used by default)
- **on_duplicate_key_update**: SQL expression for `ON DUPLICATE KEY UPDATE` clause. Effective only for "merge", "merge_direct" modes (string, optional)

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
  * Transactional: No. If fails, the target table could be dropped (because MySQL can't rollback DDL).
  * Resumable: No.
* **merge**:
  * Behavior: This mode writes rows to some intermediate tables first. If all those tasks run correctly, runs `INSERT INTO <target_table> SELECT * FROM <intermediate_table_1> UNION ALL SELECT * FROM <intermediate_table_2> UNION ALL ... ON DUPLICATE KEY UPDATE ...` query. Namely, if primary keys of a record in the intermediate tables already exist in the target table, the target record is updated by the intermediate record, otherwise the intermediate record is inserted. If the target table doesn't exist, it is created automatically.
  * Transactional: Yes.
  * Resumable: Yes.
* **merge_direct**:
  * Behavior: This mode inserts rows to the target table directly using `INSERT INTO ... ON DUPLICATE KEY UPDATE ...` query. If the target table doesn't exist, it is created automatically.
  * Transactional: No.
  * Resumable: No.

### Supported types

|database type|default value_type|note|
|:--|:--|:--|
|BIT|boolean||
|TINYINT|byte||
|SMALLINT|short||
|INT|int||
|BIGINT|long||
|FLOAT|float||
|DOUBLE|double||
|DECIMAL|decimal||
|CHAR|string||
|VARCHAR|string||
|TEXT|string||
|DATE|date||
|DATETIME|timestamp||
|TIMESTAMP|timestamp||
|TIME|time||

You can use other types by specifying `value_type` in `column_options`.

### Example

```yaml
out:
  type: mysql
  host: localhost
  user: root
  password: ""
  database: my_database
  table: my_table
  mode: insert
```

Advanced configuration:

```yaml
out:
  type: mysql
  host: localhost
  user: root
  password: ""
  database: my_database
  table: my_table
  options: {connectTimeout: 20000}
  mode: insert_direct
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
