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
- **host**: database host name (string, required if url is not set or insert_method is "oci")
- **port**: database port number (integer, default: 1521)
- **user**: database login user name (string, required)
- **password**: database login password (string, default: "")
- **database**: destination database name (string, required if url is not set or insert_method is "oci")
- **url**: URL of the JDBC connection (string, optional)
- **table**: destination table name (string, required)
- **mode**: "replace" or "insert" (string, required)
- **insert_method**: see below
- **batch_size**: size of a single batch insert (integer, default: 16777216)
- **options**: extra connection properties (hash, default: {})

insert_method supports three options.

"normal" means normal insert (default). It requires Oracle JDBC driver.

"direct" means direct path insert. It is faster than 'normal.
It requires Oracle JDBC driver too, but the version 12 driver doesn't work (the version 11 driver works).

"oci" means direct path insert using OCI(Oracle Call Interface). It is fastest.
It requires both Oracle JDBC driver and Oracle Instant Client (version 12.1.0.2.0).
You must set the library loading path to the OCI library.

If you use "oci", platform dependent library written in cpp is required.
Windows(x64) library and Linux(x64) are bundled, but others are not bundled.
You should build by yourself and set the library loading path to it.


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

#### Build environment for native library

For Windows (x64)

(1) Install JDK.

(2) Install Microsoft Visual Studio (only 2010 is tested).

(3) Install Oracle Instant Client SDK 11.1.0.6.0 for Microsoft Windows (x64).

(4) Set environment variables.

* JAVA_HOME
* OCI\_SDK_PATH ("sdk" directory of Oracle Instant Client)

(5) Open src/main/cpp/win/embulk-output-oracle.sln by Visual Studio and build.

For Windows command line, the following are needed in addition to (1) - (4).

(6) Set environment variables.

* MSVC_PATH (ex. C:\Program Files (x86)\Microsoft Visual Studio 10.0\VC)
* MSSDK_PATH (ex. C:\Program Files (x86)\Microsoft SDKs\Windows\v7.0A)

(7) Execute src/main/cpp/win/build.bat .


For Linux (x64) (only Ubuntu Server 14.04 is tested)

(1) Install JDK.

(2) Install gcc and g++ .

(3) Install Oracle Instant Client Basic and SDK 11.1.0.6.0 for Linux (x64).

(4) Create symbolic links of OCI libraries.

    ln -s libocci.so.11.1 libocci.so
    ln -s libclntsh.so.11.1 libclntsh.so

(5) Set environment variables.

* JAVA_HOME
* OCI_PATH (the directory of Oracle Instant Client Basic and the parent of the "sdk" directory)

(6) Execute src/main/cpp/linux/build.sh .
