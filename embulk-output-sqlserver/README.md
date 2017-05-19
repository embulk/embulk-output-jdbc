# SQL Server output plugin for Embulk

SQL Server output plugin for Embulk loads records to SQL Server.

## Overview

* **Plugin type**: output
* **Load all or nothing**: depends on the mode. see below.
* **Resume supported**: depends on the mode. see below.

## Configuration

- **driver_path**: path to the jar file of Microsoft SQL Server JDBC driver. If not set, open-source driver (jTDS driver) is used (string)
- **host**: database host name (string, required)
- **port**: database port number (integer, default: 1433)
- **integratedSecutiry**: whether to use integrated authentication or not. The `sqljdbc_auth.dll` must be located on Java library path if using integrated authentication. : (boolean, default: false)
```
rem C:\drivers\sqljdbc_auth.dll
embulk "-J-Djava.library.path=C:\drivers" run input-sqlserver.yml
```
- **user**: database login user name (string, required if not using integrated authentication)
- **password**: database login password (string, default: "")
- **instance**: destination instance name (string, default: use the default instance)
- **database**: destination database name (string, default: use the default database)
- **url**: URL of the JDBC connection (string, optional)
- **table**: destination table name (string, required)
- **options**: extra connection properties (hash, default: {})
- **retry_limit** max retry count for database operations (integer, default: 12)
- **retry_wait** initial retry wait time in milliseconds (integer, default: 1000 (1 second))
- **max_retry_wait** upper limit of retry wait, which will be doubled at every retry (integer, default: 1800000 (30 minutes))
- **mode**: "insert", "insert_direct", "truncate_insert" , "replace" or "merge". See below. (string, required)
- **merge_keys**: key column names for merging records in merge mode (string array, required in merge mode if table doesn't have primary key)
- **merge_rule**: list of column assignments for updating existing records used in merge mode, for example `foo = T.foo + S.foo` (`T` means target table and `S` means source table). (string array, default: always overwrites with new values)
- **insert_method**: see below
- **batch_size**: size of a single batch insert (integer, default: 16777216)
- **default_timezone**: If input column type (embulk type) is timestamp, this plugin needs to format the timestamp into a SQL string. This default_timezone option is used to control the timezone. You can overwrite timezone for each columns using column_options option. (string, default: `UTC`)
- **column_options**: advanced: a key-value pairs where key is a column name and value is options for the column.
  - **type**: type of a column when this plugin creates new tables (e.g. `VARCHAR(255)`, `INTEGER NOT NULL UNIQUE`). This used when this plugin creates intermediate tables (insert, insert_truncate and merge modes), when it creates the target table (insert_direct, merge_direct and replace modes), and when it creates nonexistent target table automatically. (string, default: depends on input column type. `BIGINT` if input column type is long, `BOOLEAN` if boolean, `DOUBLE PRECISION` if double, `CLOB` if string, `TIMESTAMP` if timestamp)
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
  * Transactional: No. If fails, the target table could be dropped (because SQL Server can't rollback DDL).
  * Resumable: No.
* **merge**:
  * Behavior: This mode writes rows to some intermediate tables first. If all those tasks run correctly, runs `MERGE INTO ... WHEN MATCHED THEN UPDATE ...  WHEN NOT MATCHED THEN INSERT ...` query. Namely, if merge keys of a record in the intermediate tables already exist in the target table, the target record is updated by the intermediate record, otherwise the intermediate record is inserted. If the target table doesn't exist, it is created automatically.
  * Transactional: Yes.
  * Resumable: Yes.

### Insert methods

insert_method supports three options.

"normal" means normal insert (default). It requires SQL Server JDBC driver.

"native" means bulk insert using native client. It is faster than "normal".
It requires both SQL Server JDBC driver and SQL Server Native Client (11.0).

### Supported types

|database type|default value_type|note|
|:--|:--|:--|
|bit|boolean||
|tinyint|byte|unsigned|
|smallint|short||
|int|int||
|bigint|long||
|decimal|decimal||
|numeric|decimal||
|smallmoney|decimal||
|money|decimal||
|real|float||
|float|double||
|char|string||
|varchar|string||
|text|string||
|nchar|nstring||
|nvarchar|nstring||
|ntext|nstring||
|xml|nstring||
|date|date||
|time|time|support 7 digits for the fractional part of the seconds|
|datetime|timestamp||
|datetime2|timestamp||
|smalldatetime|timestamp||

You can use other types by specifying `value_type` in `column_options`.

### Example

```yaml
out:
  type: sqlserver
  driver_path: C:\drivers\sqljdbc41.jar
  host: localhost
  user: myuser
  password: ""
  instance: MSSQLSERVER
  database: my_database
  table: my_table
  mode: insert
```

Advanced configuration:

```yaml
out:
  type: sqlserver
  driver_path: C:\drivers\sqljdbc41.jar
  host: localhost
  user: myuser
  password: ""
  instance: MSSQLSERVER
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
