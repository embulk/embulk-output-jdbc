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

- **driver_path**: path to the jar file of the JDBC driver (e.g. 'sqlite-jdbc-3.8.7.jar') (string, optional)
- **driver_class**: class name of the JDBC driver (e.g. 'org.sqlite.JDBC') (string, required)
- **url**: URL of the JDBC connection (e.g. 'jdbc:sqlite:mydb.sqlite3') (string, required)
- **user**: database login user name (string, optional)
- **password**: database login password (string, optional)
- **schema**: destination schema name (string, default: use default schema)
- **table**: destination table name (string, required)
- **mode**: "replace" or "insert" (string, required)
- **batch_size**: size of a single batch insert (integer, default: 16777216)
- **options**: extra JDBC properties (hash, default: {})

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
