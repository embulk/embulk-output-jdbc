# MySQL output plugins for Embulk

MySQL output plugins for Embulk loads records to MySQL.

## Overview

* **Plugin type**: output
* **Load all or nothing**: depnds on the mode:
  * **insert**: no
  * **replace**: yes
* **Resume supported**: no

## Configuration

- **host**: database host name (string, required)
- **port**: database port number (integer, default: 3306)
- **user**: database login user name (string, required)
- **password**: database login password (string, default: "")
- **database**: destination database name (string, required)
- **table**: destination table name (string, required)
- **mode**: "replace" or "insert" (string, required)
- **batch_size**: size of a single batch insert (integer, default: 16777216)
- **options**: extra connection properties (hash, default: {})

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

### Build

```
$ ./gradlew gem
```
