# MiniFlake

A local Snowflake emulator powered by DuckDB. Drop-in replacement for development and testing.

## Quick Start

**Docker:**
```bash
docker run -p 8084:8084 miniflakedb/miniflake
```

**Binary:**
```bash
./miniflake --port 8084
```

**Docker Compose:**
```bash
docker compose up
```

## Connect

MiniFlake speaks the Snowflake HTTP protocol. Point any Snowflake driver at `localhost:8084`.

**Go (gosnowflake):**
```go
cfg := &gosnowflake.Config{
    Account:  "miniflake",
    User:     "test",
    Password: "test",
    Host:     "localhost",
    Port:     8084,
    Protocol: "http",
}
dsn, _ := gosnowflake.DSN(cfg)
db, _ := sql.Open("snowflake", dsn)
```

**Python (snowflake-connector-python):**
```python
import snowflake.connector

conn = snowflake.connector.connect(
    account="miniflake",
    user="test",
    password="test",
    host="localhost",
    port=8084,
    protocol="http",
)
```

**JDBC:**
```
jdbc:snowflake://localhost:8084/?account=miniflake&user=test&password=test&protocol=http
```

## Features

| Category | Feature | Status |
|---|---|---|
| **SQL Core** | SELECT, JOIN, subqueries | :white_check_mark: |
| | Aggregations, window functions | :white_check_mark: |
| | CTEs, UNION, INTERSECT, EXCEPT | :white_check_mark: |
| | QUALIFY clause | :white_check_mark: |
| | LATERAL FLATTEN | :white_check_mark: |
| **DDL/DML** | CREATE/DROP TABLE, VIEW, SCHEMA, DATABASE | :white_check_mark: |
| | INSERT, UPDATE, DELETE, MERGE | :white_check_mark: |
| | CREATE TABLE AS SELECT | :white_check_mark: |
| | COPY INTO | :white_check_mark: |
| **Semi-structured** | VARIANT, OBJECT, ARRAY types | :white_check_mark: |
| | Dot notation, bracket notation | :white_check_mark: |
| | PARSE_JSON, OBJECT_CONSTRUCT, ARRAY_AGG | :white_check_mark: |
| **Stages** | Internal named stages | :white_check_mark: |
| | PUT / GET commands | :white_check_mark: |
| | Stage file listing (LS) | :white_check_mark: |
| **Streams & Tasks** | Table streams (CDC) | :construction: |
| | Task scheduling | :construction: |
| **Time Travel** | AT / BEFORE queries | :construction: |
| | UNDROP | :construction: |
| **Cloning** | Zero-copy clone | :construction: |
| **UDFs** | SQL UDFs | :white_check_mark: |
| | JavaScript UDFs | :construction: |
| **RBAC** | Roles, grants (parsed, not enforced) | :white_check_mark: |
| **Snowpipe** | REST API ingest | :construction: |
| **Information Schema** | TABLES, COLUMNS, SCHEMATA views | :white_check_mark: |

:white_check_mark: Implemented | :construction: Planned

## Architecture

```
Client (gosnowflake / Python / JDBC)
          |
          v
   +--------------+
   | HTTP Router  |  Snowflake-compatible REST API
   +--------------+
          |
          v
   +--------------+
   | SQL Rewriter |  Snowflake SQL -> DuckDB SQL
   +--------------+
          |
          v
   +--------------+
   | DuckDB Engine|  Execution + storage
   +--------------+
          |
          v
     /data  /stages
```

## Configuration

| Flag | Default | Description |
|---|---|---|
| `--port` | `8084` | HTTP listen port |
| `--data-dir` | `./data` | DuckDB database directory |
| `--stage-dir` | `./stages` | Internal stages directory |
| `--log-level` | `info` | Log level (debug, info, warn, error) |
| `--read-only` | `false` | Read-only mode |

## Development

```bash
make build        # Build binary
make test         # Run tests
make docker       # Build Docker image
make lint         # Run linter
```

## License

[MIT](LICENSE)
