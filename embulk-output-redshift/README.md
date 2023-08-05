# Redshift output plugin for Embulk

Redshift output plugin for Embulk loads records to Redshift.

## Overview

* **Plugin type**: output
* **Load all or nothing**: depends on the mode. see below.
* **Resume supported**: depends on the mode. see below.

## Configuration

- **host**: database host name (string, required)
- **port**: database port number (integer, default: 5439)
- **user**: database login user name (string, required)
- **ssl**: use SSL to connect to the database (string, default: "disable". "enable" uses SSL without server-side validation and "verify" checks the certificate. For compatibility reasons, "true" behaves as "enable" and "false" behaves as "disable".)
- **password**: database login password (string, default: "")
- **database**: destination database name (string, required)
- **schema**: destination schema name (string, default: "public")
- **temp_schema**: schema name for intermediate tables. by default, intermediate tables will be created in the schema specified by `schema`. replace mode doesn't support temp_schema. (string, optional)
- **table**: destination table name (string, required)
- **create_table_constraint**: table constraint added to `CREATE TABLE` statement, like `CREATE TABLE <table_name> (<column1> <type1>, <column2> <type2>, ..., <create_table_constraint>) <create_table_option>`.
- **create_table_option**: table option added to `CREATE TABLE` statement, like `CREATE TABLE <table_name> (<column1> <type1>, <column2> <type2>, ..., <create_table_constraint>) <create_table_option>`.
- **transaction_isolation**: transaction isolation level for each connection ("read_uncommitted", "read_committed", "repeatable_read" or "serializable"). if not specified, database default value will be used.
- **access_key_id**: deprecated. `aws_access_key_id` should be used (see "basic" in `aws_auth_method`).
- **secret_access_key**: deprecated. `aws_secret_access_key` should be used (see "basic" in `aws_auth_method`).
- **aws_auth_method**: name of mechanism to authenticate requests ("basic", "env", "instance", "profile", "properties", "anonymous", "session" or "default". default: "basic")

  - "basic": uses `access_key_id` and `secret_access_key` to authenticate.

    - **aws_access_key_id**: AWS access key ID (string, required)

    - **aws_secret_access_key**: AWS secret access key (string, required)

  - "env": uses `AWS_ACCESS_KEY_ID` (or `AWS_ACCESS_KEY`) and `AWS_SECRET_KEY` (or `AWS_SECRET_ACCESS_KEY`) environment variables.

  - "instance": uses EC2 instance profile.

  - "profile": uses credentials written in a file. Format of the file is as following, where `[...]` is a name of profile.

    - **aws_profile_file**: path to a profiles file. (string, default: given by `AWS_CREDENTIAL_PROFILES_FILE` environment varialbe, or ~/.aws/credentials).

    - **aws_profile_name**: name of a profile. (string, default: `"default"`)

    ```
    [default]
    aws_access_key_id=YOUR_ACCESS_KEY_ID
    aws_secret_access_key=YOUR_SECRET_ACCESS_KEY

    [profile2]
    ...
    ```

  - "properties": uses `aws.accessKeyId` and `aws.secretKey` Java system properties.

  - "anonymous": uses anonymous access. This authentication method can access only public files.

  - "session": uses temporary-generated `access_key_id`, `secret_access_key` and `session_token`.

    - **aws_access_key_id**: AWS access key ID (string, required)

    - **aws_secret_access_key**: AWS secret access key (string, required)

    - **aws_session_token**: session token (string, required)

  - "default": uses AWS SDK's default strategy to look up available credentials from runtime environment. This method behaves like the combination of the following methods.

    1. "env"
    1. "properties"
    1. "profile"
    1. "instance"

- **iam_user_name**: IAM user name for uploading temporary files to S3. The user should have permissions of `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, `s3:ListBucket` and `sts:GetFederationToken`. And furthermore, the user should have permission of `s3:GetBucketLocation` if Redshift region and S3 bucket region are different. (string, default: "", but we strongly recommend that you use IAM user for security reasons. see below.)
- **s3_bucket**: S3 bucket name for temporary files
- **s3_key_prefix**: S3 key prefix for temporary files (string, default: "")
- **delete_s3_temp_file**: whether to delete temporary files uploaded on S3 (boolean, default: true)
- **copy_iam_role_name**: IAM Role for COPY credential(https://docs.aws.amazon.com/redshift/latest/dg/copy-usage_notes-access-permissions.html), if this is set, IAM Role is used instead of aws access key and aws secret access key(string, optional)
- **copy_aws_account_id**: IAM Role's account ID for multi account COPY. If this is set, the ID is used instead of authenticated user's account ID. This is enabled only if copy_iam_role_name is set.(string, optional)
- **options**: extra connection properties (hash, default: {})
- **retry_limit**: max retry count for database operations (integer, default: 12). When intermediate table to create already created by another process, this plugin will retry with another table name to avoid collision.
- **retry_wait**: initial retry wait time in milliseconds (integer, default: 1000 (1 second))
- **max_retry_wait**: upper limit of retry wait, which will be doubled at every retry (integer, default: 1800000 (30 minutes))
- **mode**: "insert", "insert_direct", "truncate_insert", "replace" or "merge". See below. (string, required)
- **merge_keys**: key column names for merging records in merge mode (string array, required in merge mode)
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
  * Resumable: No.
* **insert_direct**:
  * Behavior: This mode inserts rows to the target table directly. If the target table doesn't exist, it is created automatically.
  * Transactional: No. If fails, the target table could have some rows inserted.
  * Resumable: No.
* **truncate_insert**:
  * Behavior: Same with `insert` mode excepting that it truncates(using `delete from`, not using `truncate`)  the target table right before the last `INSERT ...` query.
  * Transactional: Yes.
  * Resumable: No.
* **replace**:
  * Behavior: This mode writes rows to an intermediate table first. If all those tasks run correctly, drops the target table and alters the name of the intermediate table into the target table name.
  * Transactional: Yes.
  * Resumable: No.
* **merge**:
  * Behavior: This mode writes rows to some intermediate tables first. If all those tasks run correctly, inserts new records from intermediate tables after updating records whose keys exist in intermediate tables. Namely, if merge keys of a record in the intermediate tables already exist in the target table, the target record is updated by the intermediate record, otherwise the intermediate record is inserted. If the target table doesn't exist, it is created automatically. NOTE: Merge does not work correctly if merge keys contain `NULL`s.
  * Transactional: Yes.
  * Resumable: No.

### Supported types

|database type|default value_type|note|
|:--|:--|:--|
|bool|boolean||
|smallint|short||
|int|int||
|bigint|long||
|real|float||
|double precision|double||
|numeric|decimal||
|char|string||
|varchar|string||
|date|date||
|timestamp|timestamp||

You can use other types by specifying `value_type` in `column_options`.

### Example

```yaml
out:
  type: redshift
  host: myinstance.us-west-2.redshift.amazonaws.com
  user: pg
  password: ""
  database: my_database
  table: my_table
  aws_access_key_id: ABCXYZ123ABCXYZ123
  aws_secret_access_key: AbCxYz123aBcXyZ123
  iam_user_name: my-s3-read-only
  s3_bucket: my-redshift-transfer-bucket
  s3_key_prefix: temp/redshift
  mode: insert
```

Advanced configuration:

```yaml
out:
  type: redshift
  host: myinstance.us-west-2.redshift.amazonaws.com
  user: pg
  ssl: enable
  password: ""
  database: my_database
  table: my_table
  aws_access_key_id: ABCXYZ123ABCXYZ123
  aws_secret_access_key: AbCxYz123aBcXyZ123
  iam_user_name: my-s3-read-only
  s3_bucket: my-redshift-transfer-bucket
  s3_key_prefix: temp/redshift
  options: {loglevel: 2}
  mode: insert_direct
  column_options:
    my_col_1: {type: 'VARCHAR(255)'}
    my_col_3: {type: 'INT NOT NULL'}
    my_col_4: {value_type: string, timestamp_format: `%Y-%m-%d %H:%M:%S %z`, timezone: '-0700'}
    my_col_5: {type: 'DECIMAL(18,9)', value_type: pass}
```

To use IAM Role:

```yaml
out:
  type: redshift
  host: myinstance.us-west-2.redshift.amazonaws.com
  user: pg
  password: ""
  database: my_database
  table: my_table
  s3_bucket: my-redshift-transfer-bucket
  s3_key_prefix: temp/redshift
  mode: insert
  aws_auth_method: instance
```

To use AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables:

```yaml
out:
  type: redshift
  host: myinstance.us-west-2.redshift.amazonaws.com
  user: pg
  password: ""
  database: my_database
  table: my_table
  s3_bucket: my-redshift-transfer-bucket
  s3_key_prefix: temp/redshift
  mode: insert
  aws_auth_method: env
```

### Build

```
$ ../gradlew gem
```

### Security
This plugin requires AWS access credentials so that it may write temporary files to S3. There are two security options, Standard and Federated.
To use Standard security, give **aws_key_id** and **secret_access_key**. To use Federated mode, also give the **iam_user_name** field.
Federated mode really means temporary credentials, so that a man-in-the-middle attack will see AWS credentials that are only valid for 1 calendar day after the transaction.
