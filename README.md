# Jdbc output plugin for Embulk

TODO: Write short description here

## Overview

* **Plugin type**: output
* **Rollback supported**: no
* **Resume supported**: no
* **Cleanup supported**: no

## Configuration

- **property1**: description (string, required)
- **property2**: description (integer, default: default-value)

## Example

```yaml
out:
  type: jdbc
  property1: example1
  property2: example2
```

## Build

```
$ ./gradlew gem
```
