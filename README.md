# MiniFlake

A local Snowflake emulator powered by DuckDB. Drop-in replacement for development and testing of code that targets Snowflake.

**Status:** in active development. The query path (login → SQL → fetch → session lifecycle) works end-to-end through the gosnowflake driver, the Python connector, and JDBC. The Snowflake-specific feature matrix below uses three markers: `✅` exercised by integration tests, `🟡` building block exists but not wired end-to-end, `🚧` planned.

## Quick Start

**Docker:**
```bash
docker run -p 8084:8084 ministackorg/miniflake
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
| **SQL Core** | SELECT, JOIN, subqueries | ✅ |
| | Aggregations, window functions | ✅ |
| | CTEs, UNION, INTERSECT, EXCEPT | ✅ |
| | QUALIFY clause | 🟡 |
| | LATERAL FLATTEN | 🟡 |
| **DDL/DML** | CREATE/DROP TABLE, VIEW, SCHEMA, DATABASE | ✅ |
| | INSERT, UPDATE, DELETE | ✅ |
| | MERGE | 🚧 |
| | CREATE TABLE AS SELECT | 🟡 |
| | COPY INTO | 🟡 |
| **Semi-structured** | VARIANT, OBJECT, ARRAY types | ✅ |
| | Dot notation, bracket notation | ✅ |
| | PARSE_JSON, OBJECT_CONSTRUCT, ARRAY_AGG | ✅ |
| **Stages** | Internal named stages (CREATE/DROP STAGE) | ✅ |
| | PUT / GET commands | 🟡 |
| | Stage file listing (LS) | 🟡 |
| **Streams & Tasks** | Table streams (CDC) | 🟡 |
| | Task scheduling | 🟡 |
| **Time Travel** | AT / BEFORE queries | 🚧 |
| | UNDROP | 🚧 |
| **Cloning** | Table / Schema / Database clone (CTAS-backed) | 🟡 |
| | Zero-copy snapshots | 🚧 |
| **UDFs** | SQL UDFs | ✅ |
| | Python UDFs | 🟡 |
| | JavaScript UDFs | 🚧 |
| **RBAC** | Roles, grants (parsed, not enforced) | ✅ |
| **Snowpipe** | REST API ingest | 🚧 |
| **Information Schema** | TABLES, COLUMNS, SCHEMATA views | ✅ |

✅ Exercised by integration tests | 🟡 Implementation exists but not wired end-to-end | 🚧 Planned

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

Requires Go 1.26+ (DuckDB binding is built via cgo, so a C toolchain is needed too — `build-essential` on Debian/Ubuntu, Xcode CLT on macOS).

```bash
make build              # Build binary at ./bin/miniflake
make test               # Unit tests with race detector
make test-integration   # Integration tests (gosnowflake → in-process server)
make docker             # Build Docker image
make fmt                # gofmt + go vet
```

CI runs the same targets on every push and PR via `.github/workflows/test.yml`. The `docker.yml` workflow builds the image (no push from PRs) and smoke-tests `/health`.

### Contributing

PRs welcome. For features that flip a 🟡 → ✅: add an integration test in `test/integration/` that exercises the feature through the gosnowflake driver before changing the README marker.

## License

[MIT](LICENSE)
