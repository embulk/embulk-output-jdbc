# Oracle output plugin for Embulk

Oracle output plugin for Embulk loads records to Oracle.

## Overview

* **Plugin type**: output
* **Load all or nothing**: depends on the mode. see below.
* **Resume supported**: depends on the mode. see below.

## Configuration

- **driver_path**: path to the jar file of the Oracle JDBC driver (string)
- **host**: database host name (string, required if url is not set or insert_method is "oci")
- **port**: database port number (integer, default: 1521)
- **user**: database login user name (string, required)
- **password**: database login password (string, default: "")
- **database**: destination database name (string, required if url is not set or insert_method is "oci")
- **url**: URL of the JDBC connection (string, optional)
- **table**: destination table name (string, required)
- **options**: extra connection properties (hash, default: {})
- **retry_limit** max retry count for database operations (integer, default: 12)
- **retry_wait** initial retry wait time in milliseconds (integer, default: 1000 (1 second))
- **max_retry_wait** upper limit of retry wait, which will be doubled at every retry (integer, default: 1800000 (30 minutes))
- **mode**: "insert", "insert_direct", "truncate_insert", "replace" or "merge". See below. (string, required)
- **merge_keys**: key column names for merging records in merge mode (string array, required in merge mode if table doesn't have primary key)
- **merge_rule**: list of column assignments for updating existing records used in merge mode, for example `foo = T.foo + S.foo` (`T` means target table and `S` means source table). (string array, default: always overwrites with new values)
- **insert_method**: see below
- **batch_size**: size of a single batch insert (integer, default: 16777216)
- **default_timezone**: If input column type (embulk type) is timestamp, this plugin needs to format the timestamp into a SQL string. This default_timezone option is used to control the timezone. You can overwrite timezone for each columns using column_options option. (string, default: `UTC`)
- **column_options**: advanced: a key-value pairs where key is a column name and value is options for the column.
  - **type**: type of a column when this plugin creates new tables (e.g. `VARCHAR(255)`, `INTEGER NOT NULL UNIQUE`). This used when this plugin creates intermediate tables (insert, truncate_insert and merge modes), when it creates the target table (insert_direct and replace modes), and when it creates nonexistent target table automatically. (string, default: depends on input column type. `BIGINT` if input column type is long, `BOOLEAN` if boolean, `DOUBLE PRECISION` if double, `CLOB` if string, `TIMESTAMP` if timestamp)
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
  * Behavior: This mode writes rows to some intermediate tables first. If all those tasks run correctly, runs `MERGE INTO ... WHEN MATCHED THEN UPDATE ...  WHEN NOT MATCHED THEN INSERT ...` query. Namely, if merge keys of a record in the intermediate tables already exist in the target table, the target record is updated by the intermediate record, otherwise the intermediate record is inserted. If the target table doesn't exist, it is created automatically.
  * Transactional: Yes.
  * Resumable: Yes.

### Insert methods

insert_method supports three options.

"normal" means normal insert (default). It requires Oracle JDBC driver.

"direct" means direct path insert. It is faster than "normal".
It requires Oracle JDBC driver too, but the version 12 driver doesn't work (the version 11 driver works).

"oci" means direct path insert using OCI(Oracle Call Interface). It is fastest.
It requires both Oracle JDBC driver and Oracle Instant Client (version 12.1.0.2.0).
You must set the library loading path to the OCI library.
And it uses an optional native library (embulk-output-oracle-oci) written in cpp to improve performance furthermore.
Not only the source codes of the library, but also the built libraries for Windows(x64) and Linux(x64) have bean committed.


### Supported types

|database type|default value_type|note|
|:--|:--|:--|
|NUMBER|decimal||
|FLOAT|double||
|CHAR|string||
|VARCHAR2|string||
|CLOB|string||
|NCHAR|nstring||
|NVARCHAR2|nstring||
|NCLOB|nstring||
|DATE|timestamp|Oracle DATE type stores date and time information.|
|TIMESTAMP|timestamp||

You can use other types by specifying `value_type` in `column_options`.

### Example

```yaml
out:
  type: oracle
  driver_path: /opt/oracle/ojdbc6.jar
  host: localhost
  user: root
  password: ""
  database: my_database
  table: my_table
  mode: insert
  insert_method: direct
```

Advanced configuration:

```yaml
out:
  type: oracle
  driver_path: /opt/oracle/ojdbc6.jar
  host: localhost
  user: root
  password: ""
  database: my_database
  table: my_table
  options: {LoginTimeout: 20000}
  mode: insert_direct
  insert_method: direct
  column_options:
    my_col_1: {type: 'VARCHAR(255)'}
    my_col_3: {type: 'INT NOT NULL'}
    my_col_4: {value_type: string, timestamp_format: `%Y-%m-%d %H:%M:%S %z`, timezone: '-0700'}
    my_col_5: {type: 'DECIMAL(18,9)', value_type: pass}
```

### Build

```
$ ./gradlew gem
```

#### Build environment for native library

For Windows (x64)

(1) Install Microsoft Visual Studio (only 2010 is tested).

(2) Install Oracle Instant Client SDK 11.1.0.6.0 for Microsoft Windows (x64).

(3) Set environment variables.

* OCI\_SDK_PATH ("sdk" directory of Oracle Instant Client)

(4) Open src/main/cpp/win/embulk-output-oracle-oci.sln by Visual Studio and build.

For Windows command line, the following are needed in addition to (1) - (4).

(5) Set environment variables.

* MSVC_PATH (ex. C:\Program Files (x86)\Microsoft Visual Studio 10.0\VC)
* MSSDK_PATH (ex. C:\Program Files (x86)\Microsoft SDKs\Windows\v7.0A)

(6) Execute src/main/cpp/win/build.bat .


For Linux (x64) (only Ubuntu Server 14.04 is tested)

(1) Install gcc and g++ .

(2) Install Oracle Instant Client Basic and SDK 11.1.0.6.0 for Linux (x64).

(3) Create symbolic links of OCI libraries.

    ln -s libocci.so.11.1 libocci.so
    ln -s libclntsh.so.11.1 libclntsh.so

(4) Set environment variables.

* OCI_PATH (the directory of Oracle Instant Client Basic and the parent of the "sdk" directory)

(5) Execute src/main/cpp/linux/build.sh .

***
<img src="https://www.yourkit.com/images/yklogo.png" alt="YourKit"/> is used to improve performance of embulk-output-oracle.
YourKit supports open source projects with its full-featured Java Profiler.
YourKit, LLC is the creator of <a href="https://www.yourkit.com/java/profiler/index.jsp">YourKit Java Profiler</a> and <a href="https://www.yourkit.com/.net/profiler/index.jsp">YourKit .NET Profiler</a>, innovative and intelligent tools for profiling Java and .NET applications.
