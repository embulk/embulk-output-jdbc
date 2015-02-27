# JDBC output plugins for Embulk

JDBC output plugins for Embulk loads records to databases using JDBC drivers.

## MySQL

See [embulk-output-mysql](embulk-output-mysql/).

## PostgreSQL

See [embulk-output-postgresql](embulk-output-postgresql/).

## Redshift

See [embulk-output-redshift](embulk-output-redshift/).

## Generic

### Overview

* **Plugin type**: output
* **Load all or nothing**: depnds on the mode:
  * **insert**: no
  * **replace**: yes
* **Resume supported**: no

### Configuration

- **host**: database host name (string, required)
- **port**: database port number (integer, required)
- **user**: database login user name (string, required)
- **password**: database login password (string, default: "")
- **database**: destination database name (string, required)
- **schema**: destination name (string, default: use default schema)
- **table**: destination name (string, required)
- **mode**: "replace" or "insert" (string, required)
- **batch_size**: size of a single batch insert (integer, default: 16777216)
- **options**: extra JDBC properties (hash, default: {})
- **driver_name**: name of the JDBC driver used in connection url (e.g. 'sqlite') (string, required)
- **driver_class**: class name of the JDBC driver (e.g. 'org.sqlite.JDBC') (string, required)
- **driver_path**: path to the jar file of the JDBC driver (e.g. 'sqlite-jdbc-3.8.7.jar') (string, optional)

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
  driver_path: /opt/oracle/ojdbc6.jar
```

### Build

```
$ ./gradlew gem
```
