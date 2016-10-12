# Generic JDBC output plugin for Embulk

Generic JDBC output plugin for Embulk loads records to a database using a JDBC driver. If the database follows ANSI SQL standards and JDBC standards strictly, this plugin works. But because of many incompatibilities, use case of this plugin is very limited. It's recommended to use specific plugins for the databases.

## Overview

* **Plugin type**: output
* **Load all or nothing**: depends on the mode. see below.
* **Resume supported**: depends on the mode. see below.

## Configuration

- **driver_path**: path to the jar file of the JDBC driver (e.g. 'sqlite-jdbc-3.8.7.jar') (string, optional)
- **driver_class**: class name of the JDBC driver (e.g. 'org.sqlite.JDBC') (string, required)
- **url**: URL of the JDBC connection (e.g. 'jdbc:sqlite:mydb.sqlite3') (string, required)
- **user**: database login user name (string, optional)
- **password**: database login password (string, optional)
- **schema**: destination schema name (string, default: use default schema)
- **table**: destination table name (string, required)
- **options**: extra JDBC properties (hash, default: {})
- **retry_limit** max retry count for database operations (integer, default: 12)
- **retry_wait** initial retry wait time in milliseconds (integer, default: 1000 (1 second))
- **max_retry_wait** upper limit of retry wait, which will be doubled at every retry (integer, default: 1800000 (30 minutes))
- **mode**: "insert", "insert_direct", "truncate_insert", or "replace". See below (string, required)
- **batch_size**: size of a single batch insert (integer, default: 16777216)
- **max_table_name_length**: maximum length of table name in this RDBMS (integer, default: 256)
- **default_timezone**: If input column type (embulk type) is timestamp, this plugin needs to format the timestamp into a SQL string. This default_timezone option is used to control the timezone. You can overwrite timezone for each columns using column_options option. (string, default: `UTC`)
- **column_options**: advanced: a key-value pairs where key is a column name and value is options for the column.
  - **type**: type of a column when this plugin creates new tables (e.g. `VARCHAR(255)`, `INTEGER NOT NULL UNIQUE`). This used when this plugin creates intermediate tables (insert and truncate_insert modes), when it creates the target table (replace mode), and when it creates nonexistent target table automatically. (string, default: depends on input column type. `BIGINT` if input column type is long, `BOOLEAN` if boolean, `DOUBLE PRECISION` if double, `CLOB` if string, `TIMESTAMP` if timestamp)
  - **value_type**: This plugin converts input column type (embulk type) into a database type to build a INSERT statement. This value_type option controls the type of the value in a INSERT statement. (string, default: depends on the sql type of the column. Available values options are: `byte`, `short`, `int`, `long`, `double`, `float`, `boolean`, `string`, `nstring`, `date`, `time`, `timestamp`, `decimal`, `json`, `null`, `pass`)
  - **timestamp_format**: If input column type (embulk type) is timestamp and value_type is `string` or `nstring`, this plugin needs to format the timestamp value into a string. This timestamp_format option is used to control the format of the timestamp. (string, default: `%Y-%m-%d %H:%M:%S.%6N`)
  - **timezone**: If input column type (embulk type) is timestamp, this plugin needs to format the timestamp value into a SQL string. In this cases, this timezone option is used to control the timezone. (string, value of default_timezone option is used by default)

## Modes

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
  * Transactional: No. If fails, the target table could be dropped.
  * Resumable: No.
* **merge**:
  * Behavior: This mode writes rows to some intermediate tables first. If all those tasks run correctly, merges the intermediate tables into the target table. Namely, if primary keys of a record in the intermediate tables already exist in the target table, the target record is updated by the intermediate record, otherwise the intermediate record is inserted. If the target table doesn't exist, it is created automatically.
  * Transactional: Yes.
  * Resumable: Yes.
* **merge_direct**:
  * Behavior: This mode merges rows to the target table directly. Namely, if primary keys of an input record already exist in the target table, the target record is updated by the input record, otherwise the input record is inserted. If the target table doesn't exist, it is created automatically.
  * Transactional: No.
  * Resumable: No.

## Example

```yaml
out:
  type: jdbc
  driver_path: /usr/local/nz/lib/nzjdbc3.jar
  driver_class: org.netezza.Driver
  url: jdbc:jdbc:netezza://127.0.0.1:5480/mydb
  user: myuser
  password: "mypassword"
  table: my_table
  mode: insert
```

Advanced configuration:

```yaml
out:
  type: jdbc
  driver_path: /usr/local/nz/lib/nzjdbc3.jar
  driver_class: org.netezza.Driver
  url: jdbc:jdbc:netezza://127.0.0.1:5480/mydb
  user: myuser
  password: "mypassword"
  table: my_table
  options: {loglevel: 2}
  mode: insert_direct
  column_options:
    my_col_1: {type: 'VARCHAR(255)'}
    my_col_3: {type: 'INT NOT NULL'}
    my_col_4: {value_type: string, timestamp_format: `%Y-%m-%d %H:%M:%S %z`, timezone: '-0700'}
    my_col_5: {type: 'DECIMAL(18,9)', value_type: pass}
```

## Build

```
$ ./gradlew gem
```
