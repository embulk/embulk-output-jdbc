# Redshift output plugins for Embulk

Redshift output plugins for Embulk loads records to Redshift.

## Overview

* **Plugin type**: output
* **Load all or nothing**: depnds on the mode:
  * **insert**: no
  * **replace**: yes
* **Resume supported**: no

## Configuration

- **host**: database host name (string, required)
- **port**: database port number (integer, default: 5439)
- **user**: database login user name (string, required)
- **password**: database login password (string, default: "")
- **database**: destination database name (string, required)
- **schema**: destination name (string, default: "public")
- **table**: destination name (string, required)
- **mode**: "replace" or "insert" (string, required)
- **batch_size**: size of a single batch insert (integer, default: 16777216)
- **options**: extra connection properties (hash, default: {})

### Example

```yaml
out:
  type: redshift
  host: myinstance.us-west-2.redshift.amazonaws.com
  user: pg
  password: ""
  database: my_database
  table: my_table
  access_key_id: ABCXYZ123ABCXYZ123
  secret_access_key: AbCxYz123aBcXyZ123
  s3_bucket: my-redshift-transfer-bucket
  iam_user_name: my-s3-read-only
  mode: insert
```

### Build

```
$ ./gradlew gem
```
