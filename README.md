# JDBC output plugins for Embulk

JDBC output plugins for Embulk loads records to databases using JDBC drivers.

## MySQL

See [embulk-output-mysql](embulk-output-mysql/).

## PostgreSQL

See [embulk-output-postgresql](embulk-output-postgresql/).

## Oracle

See [embulk-output-oracle](embulk-output-oracle/).

## Redshift

See [embulk-output-redshift](embulk-output-redshift/).

## Generic

### Overview

* **Plugin type**: output
* **Load all or nothing**: depnds on the mode. see bellow.
* **Resume supported**: depnds on the mode. see bellow.

### Configuration

- **driver_path**: path to the jar file of the JDBC driver (e.g. 'sqlite-jdbc-3.8.7.jar') (string, optional)
- **driver_class**: class name of the JDBC driver (e.g. 'org.sqlite.JDBC') (string, required)
- **url**: URL of the JDBC connection (e.g. 'jdbc:sqlite:mydb.sqlite3') (string, required)
- **user**: database login user name (string, optional)
- **password**: database login password (string, optional)
- **schema**: destination schema name (string, default: use default schema)
- **table**: destination table name (string, required)
- **options**: extra JDBC properties (hash, default: {})
- **mode**: "insert", "insert_direct", "truncate_insert", or "replace". See bellow (string, required)
- **batch_size**: size of a single batch insert (integer, default: 16777216)
- **timestamp_format**: strftime(3) format when embulk writes a timestamp value to a VARCHAR or CLOB column (string, default: `%Y-%m-%d %H:%M:%S.%6N`)
- **max_table_name_length**: maximum length of table name in this RDBMS (integer, default: 256)
- **default_timezone**: If input column type (embulk type) is timestamp and destination column type is `string` or `nstring`, this plugin needs to format the timestamp into a string. This default_timezone option is used to control the timezone. You can overwrite timezone for each columns using column_options option. (string, default: `UTC`)
- **column_options**: advanced: a key-value pairs where key is a column name and value is options for the column.
  - **type**: type of a column when this plugin creates new tables (e.g. `VARCHAR(255)`, `INTEGER NOT NULL UNIQUE`). This used when this plugin creates intermediate tables (insert and truncate_insert modes), when it creates the target table (replace mode), and when it creates nonexistent target table automatically. (string, default: depends on input column type. `BIGINT` if input column type is long, `BOOLEAN` if boolean, `DOUBLE PRECISION` if double, `CLOB` if string, `TIMESTAMP` if timestamp)
  - **value_type**: This plugin converts input column type (embulk type) into a database type to build a INSERT statement. This value_type option controls the type of the value in a INSERT statement. (string, default: depends on input column type. Available values options are: `byte, `short`, `int`, `long`, `double`, `float`, `boolean`, `string`, `nstring`, `date`, `time`, `timestamp`, `decimal`, `null`, `pass`)
  - **timestamp_format**: If input column type (embulk type) is timestamp and value_type is `string` or `nstring`, this plugin needs to format the timestamp value into a string. This timestamp_format option is used to control the format of the timestamp. (string, default: `%Y-%m-%d %H:%M:%S.%6N`)
  - **timezone**: If input column type (embulk type) is timestamp and value_type is `string` or `nstring`, this plugin needs to format the timestamp value into a string. And if the input column type is timestamp and value_type is `date`, this plugin needs to consider timezone. In those cases, this timezone option is used to control the timezone. (string, value of default_timezone option is used by default)

### Modes

* **insert**:
  * Behavior: This mode writes rows to some intermediate tables first. If all those tasks run correctly, runs `INSERT INTO <target_table> SELECT * FROM <intermediate_table_1> UNION ALL SELECT * FROM <intermediate_table_2> UNION ALL ...` query.
  * Transactional: Yes. This mode successfully writes all rows, or fails with writing zero rows.
  * Resumable: Yes.
* **insert_direct**:
  * Behavior: This mode inserts rows to the target table directly.
  * Transactional: No. If fails, the target table could have some rows inserted.
  * Resumable: No.
* **truncate_insert**:
  * Behavior: Same with `insert` mode excepting that it truncates the target table right before the lst `INSERT ...` query.
  * Transactional: Yes.
  * Resumable: Yes.
* **replace**:
  * Behavior: Same with `insert` mode excepting that it truncates the target table right before the lst `INSERT ...` query.
  * Transactional: Yes.
  * Resumable: No.

### Example

```yaml
out:
  type: jdbc
  driver_path: /opt/oracle/ojdbc6.jar
  driver_class: oracle.jdbc.driver.OracleDriver
  url: jdbc:oracle:thin:@127.0.0.1:1521:mydb
  user: myuser
  password: "mypassword"
  table: my_table
  mode: insert
```

### Build

```
$ ./gradlew gem
```
