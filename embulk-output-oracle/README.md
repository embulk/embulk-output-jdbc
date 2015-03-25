# Oracle output plugins for Embulk

Oracle output plugins for Embulk loads records to Oracle.

## Overview

* **Plugin type**: output
* **Load all or nothing**: depends on the mode:
  * **insert**: no
  * **replace**: yes
* **Resume supported**: no

## Configuration

- **driver_path**: path to the jar file of the Oracle JDBC driver (string)
- **host**: database host name (string, required if url is not set)
- **port**: database port number (integer, default: 1521)
- **user**: database login user name (string, required)
- **password**: database login password (string, default: "")
- **database**: destination database name (string, required if url is not set)
- **url**: URL of the JDBC connection (string, optional)
- **table**: destination table name (string, required)
- **mode**: "replace" or "insert" (string, required)
- **insert_method**: see below
- **batch_size**: size of a single batch insert (integer, default: 16777216)
- **options**: extra connection properties (hash, default: {})

'insert_method' supports three options.

'normal' means normal insert (default). It requires Oracle Thin JDBC driver.
'direct' means direct path insert. It is faster than 'normal.
It requires Oracle thin JDBC driver too, but ojdbc7.jar doesn't work.
'oci' means direct path insert using OCI(Oracle Call Interface). It is fastest.
It requires both Oracle thin JDBC driver and Oracle Instant Client.


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

### Build

```
$ ./gradlew gem
```
