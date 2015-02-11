# JDBC output plugins for Embulk

JDBC output plugins for Embulk loads records to databases using JDBC drivers.

## MySQL

See [embulk-output-mysql/README.md](embulk-output-mysql/).

## PostgreSQL

See [embulk-output-postgresql/README.md](embulk-output-postgresql/).

## Generic JDBC databases

### Overview

* **Plugin type**: output
* **Rollback supported**: no
* **Resume supported**: no
* **Cleanup supported**: no

### Configuration

- **host**: database host name (string, required)
- **port**: database port number (integer, required)
- **user**: database login user name (string, required)
- **password**: database login password (string, default: "")
- **database**: destination database name (string, required)
- **schema**: destination name (string, default: null)
- **table**: destination name (string, required)
- **mode**: "replace" or "insert" (string, required)
- **batch_size**: size of a single batch insert (integer, default: 16777216)
- **optoins**: extra JDBC properties (hash, default: {})
- **driver_name**: name of the JDBC driver used in connection url (e.g. 'sqlite') (string, required)
- **driver_name**: class name of the JDBC driver (e.g. 'org.sqlite.JDBC') (string, required)

### Example

In addtion to the configuration, you need to supply -C option to embulk command to add jar files to the classpath.

```yaml
out:
  type: jdbc
  host: localhost
  port: 1521
  user: myuser
  password: ""
  database: my_database
  table: my_table
  mode: insert
  driver_name: oracle
  driver_class: oracle.jdbc.driver.OracleDriver
```

### Build

```
$ ./gradlew gem
```
