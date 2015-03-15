# Oracle output plugins for Embulk

Oracle output plugins for Embulk loads records to Oracle.

## Overview

* **Plugin type**: output
* **Load all or nothing**: depnds on the mode:
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
- **batch_size**: size of a single batch insert (integer, default: 16777216)
- **options**: extra connection properties (hash, default: {})


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
```

### Build

```
$ ./gradlew gem
```
