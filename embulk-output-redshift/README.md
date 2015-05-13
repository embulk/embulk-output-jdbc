# Redshift output plugins for Embulk

Redshift output plugins for Embulk loads records to Redshift.

## Overview

* **Plugin type**: output
* **Load all or nothing**: depnds on the mode. see bellow.
* **Resume supported**: depnds on the mode. see bellow.

## Configuration

- **host**: database host name (string, required)
- **port**: database port number (integer, default: 5439)
- **user**: database login user name (string, required)
- **password**: database login password (string, default: "")
- **database**: destination database name (string, required)
- **schema**: destination schema name (string, default: "public")
- **table**: destination table name (string, required)
- **mode**: "replace" or "insert" (string, required)
- **batch_size**: size of a single batch insert (integer, default: 16777216)
- **timestamp_format**: strftime(3) format when embulk writes a timestamp value to a VARCHAR or CLOB column (string, default: `%Y-%m-%d %H:%M:%S.%6N`)
- **timezone**: timezone used to format a timestamp value using `timestamp_format` (string, default: UTC)
- **options**: extra connection properties (hash, default: {})
- **string_pass_through**: if a value is string, writes it to database without conversion regardless of the target table's column type. Usually, if the target table's column type is not string while the value is string, embulk writes NULL. But if this option is true, embulk lets the database parse the string into the column type. If the conversion fails, the task fails. (boolean, default: false)

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
* **merge**:
  * Behavior: This mode writes rows to some intermediate tables first. If all those tasks run correctly, runs `INSERT INTO <target_table> SELECT * FROM <intermediate_table_1> UNION ALL SELECT * FROM <intermediate_table_2> UNION ALL ... ON DUPLICATE KEY UPDATE ...` query.
  * Transactional: Yes.
  * Resumable: Yes.
* **merge_direct**:
  * Behavior: This mode inserts rows to the target table directory using `INSERT INTO ... ON DUPLICATE KEY UPDATE ...` query.
  * Transactional: No.
  * Resumable: No.
* **replace**:
  * Behavior: Same with `insert` mode excepting that it truncates the target table right before the lst `INSERT ...` query.
  * Transactional: Yes.
  * Resumable: No.

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
