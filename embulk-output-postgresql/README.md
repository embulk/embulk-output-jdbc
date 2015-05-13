# PostgreSQL output plugins for Embulk

PostgreSQL output plugins for Embulk loads records to PostgreSQL.

## Overview

* **Plugin type**: output
* **Load all or nothing**: depnds on the mode:
  * **insert**: no
  * **replace**: yes
* **Resume supported**: no

## Configuration

- **host**: database host name (string, required)
- **port**: database port number (integer, default: 5432)
- **user**: database login user name (string, required)
- **password**: database login password (string, default: "")
- **database**: destination database name (string, required)
- **schema**: destination schema name (string, default: "public")
- **table**: destination table name (string, required)
- **mode**: "replace", "merge" or "insert" (string, required)
- **batch_size**: size of a single batch insert (integer, default: 16777216)
- **timestamp_format**: strftime(3) format when embulk writes a timestamp value to a VARCHAR or CLOB column (string, default: `%Y-%m-%d %H:%M:%S.%6N`)
- **timezone**: timezone used to format a timestamp value using `timestamp_format` (string, default: UTC)
- **options**: extra connection properties (hash, default: {})
- **string_pass_through**: if a value is string, writes it to database without conversion regardless of the target table's column type. Usually, if the target table's column type is not string while the value is string, embulk writes NULL. But if this option is true, embulk lets the database parse the string into the column type. If the conversion fails, the task fails. (boolean, default: false)

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

### Build

```
$ ./gradlew gem
```
