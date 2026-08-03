<h1 align="center">MiniFlake</h1>
<p align="center"><strong>Free, open-source local Snowflake emulator.</strong></p>
<p align="center">Snowflake wire protocol on a single port · gosnowflake / Python / JDBC compatible · DuckDB engine · MIT licensed</p>

<p align="center">
  <a href="https://github.com/ministackorg/miniflake/actions"><img src="https://img.shields.io/github/actions/workflow/status/ministackorg/miniflake/test.yml?branch=main" alt="Build"></a>
  <a href="https://github.com/ministackorg/miniflake/pkgs/container/miniflake"><img src="https://img.shields.io/badge/ghcr.io-ministackorg%2Fminiflake-blue?logo=github" alt="GHCR"></a>
  <a href="https://github.com/ministackorg/miniflake/blob/main/LICENSE"><img src="https://img.shields.io/github/license/ministackorg/miniflake" alt="License"></a>
  <img src="https://img.shields.io/badge/go-1.26-blue" alt="Go">
  <img src="https://img.shields.io/badge/duckdb-1.8-yellow" alt="DuckDB">
  <img src="https://img.shields.io/badge/status-active%20development-orange" alt="Status">
</p>

<p align="center">
  <a href="https://ministack.org">ministack.org</a> · part of the <a href="https://github.com/ministackorg">ministackorg</a> family
</p>

---

## Why MiniFlake?

Local Snowflake is hard. Snowflake's official testing options are either expensive (real account against a paid warehouse) or absent (no `docker run snowflake`). Most teams end up faking it with raw DuckDB or Postgres and writing dialect-conversion layers in their tests.

MiniFlake speaks the **actual Snowflake HTTP wire protocol** on top of DuckDB. Your application code uses the unmodified `gosnowflake` / `snowflake-connector-python` / JDBC drivers, just pointed at `localhost:8084` instead of `account.snowflakecomputing.com`. The driver does its handshake, the rewriter translates Snowflake SQL to DuckDB SQL, and DuckDB executes — same DSL on both sides means the same query that works locally works against the real warehouse.

- **Drop-in for the Snowflake drivers** — gosnowflake, snowflake-connector-python, JDBC. No app code changes, just the endpoint.
- **DuckDB engine** — fast in-process analytics, real query planning, real window functions, real CTEs.
- **Snowflake-specific surface** — `MERGE`, `LATERAL FLATTEN`, `QUALIFY`, `VARIANT`/`OBJECT`/`ARRAY`, `PARSE_JSON`, `OBJECT_CONSTRUCT`, semi-structured access (`col:key`, `col[0]`).
- **Streams, Tasks, Pipes, Time Travel, UNDROP** — the Snowflake control plane features your DBT/CDC/orchestrator code actually depends on.
- **Single binary, single port** — no `docker compose` sprawl, no per-feature containers.
- **MIT licensed** — use it, fork it, contribute to it. No paid tier.

---

## Quick Start

```bash
# Option 1: Docker (image published to GHCR on every push to main and on
# every v* tag — see .github/workflows/docker.yml)
docker run -p 8084:8084 ghcr.io/ministackorg/miniflake:latest
# Runs on http://localhost:8084 — use --port to change

# Option 2: Clone and build (required for macOS — the GitHub release
# binaries cover linux/amd64; macOS users build locally or use Docker)
git clone https://github.com/ministackorg/miniflake
cd miniflake
make build
./bin/miniflake

# Option 3: docker compose
docker compose up -d

# Verify
curl http://localhost:8084/_miniflake/health
```

That's it. No account, no API key, no sign-up.

---

## Connect

MiniFlake speaks the Snowflake HTTP wire protocol. Point any Snowflake driver at `localhost:8084`.

**Go (gosnowflake):**

```go
cfg := &gosnowflake.Config{
    Account:  "miniflake",
    User:     "test",
    Password: "test",
    Host:     "localhost",
    Port:     8084,
    Protocol: "http",
    Database: "TESTDB",
    Schema:   "PUBLIC",
}
dsn, _ := gosnowflake.DSN(cfg)
db, _ := sql.Open("snowflake", dsn)
```

**Python (snowflake-connector-python):**

```python
import snowflake.connector

conn = snowflake.connector.connect(
    account="miniflake",
    user="test", password="test",
    host="localhost", port=8084, protocol="http",
    database="TESTDB", schema="PUBLIC",
)
```

**JDBC:**

```
jdbc:snowflake://localhost:8084/?account=miniflake&user=test&password=test&protocol=http&db=TESTDB&schema=PUBLIC
```

The first two fields (account/user/password) are not validated — MiniFlake doesn't enforce credentials. They're accepted because the drivers refuse to connect without them.

### HTTP and HTTPS

MiniFlake serves **both plain HTTP and HTTPS on the same port** (`8084`). The examples above use `protocol=http`, which is the simplest path for gosnowflake, the Python connector, and JDBC. Drivers that require TLS — notably the **Snowflake .NET connector**, which has no plain-HTTP mode — connect over HTTPS to the same port with no extra flags.

On first start MiniFlake generates a self-signed certificate and persists it at `<data-dir>/miniflake-cert.pem` (stable across restarts). Strict clients validate the chain, so import that certificate into your OS or runtime trust store once. To use your own certificate instead, pass `--tls-cert` and `--tls-key`.

```bash
# HTTPS health check against the same port (–k trusts the self-signed cert)
curl -k https://localhost:8084/_miniflake/health
```

---

## Internal API

MiniFlake exposes a small internal surface for test automation:

```bash
# Health check
curl http://localhost:8084/_miniflake/health

# Reset all state between CI runs (no process restart)
curl -X POST http://localhost:8084/_miniflake/reset

# Snowpipe REST ingest
curl -X POST http://localhost:8084/v1/data/pipes/TESTDB/PUBLIC/my_pipe/insertFiles \
  -H "Content-Type: application/json" \
  -d '{"files":[{"path":"data/file1.csv","size":1024}]}'
```

To set the data directory, stage directory, or port, use flags or env vars at startup:

```bash
./miniflake --port 9000 --data-dir /var/miniflake/data --stage-dir /var/miniflake/stages
```

---

## Feature Matrix

Marker semantics — same as ministack, so the rule is rigid:

- ✅ — exercised by an integration test that runs against the real driver
- 🟡 — implementation exists in the codebase but isn't wired through end-to-end (yet)
- 🚧 — planned, not implemented

If a row says ✅, there's a Go file in `test/integration/` that uses `gosnowflake` to drive it.

| Category | Feature | Status |
|---|---|---|
| **SQL Core** | `SELECT`, `JOIN`, subqueries | ✅ |
| | Aggregations, window functions | ✅ |
| | CTEs, `UNION`, `INTERSECT`, `EXCEPT` | ✅ |
| | `QUALIFY` clause | ✅ |
| | `LATERAL FLATTEN(input => arr) AS t` (alias-bound `t.value`) | ✅ |
| **DDL/DML** | `CREATE/DROP TABLE`, `VIEW`, `SCHEMA`, `DATABASE` | ✅ |
| | `INSERT`, `UPDATE`, `DELETE` | ✅ |
| | `MERGE INTO ... USING ... WHEN MATCHED/NOT MATCHED` (upsert, single-key) | ✅ |
| | `CREATE TABLE AS SELECT` | ✅ |
| | `COPY INTO` (CSV/JSON/Parquet via stage) | ✅ |
| **Semi-structured** | `VARIANT`, `OBJECT`, `ARRAY` types | ✅ |
| | Dot notation (`col:key`), bracket notation (`col[0]`) | ✅ |
| | `PARSE_JSON`, `OBJECT_CONSTRUCT`, `ARRAY_AGG` | ✅ |
| **Stages** | Internal named stages (`CREATE/DROP STAGE`) | ✅ |
| | `PUT` / `GET` commands — server-side handler works; gosnowflake intercepts client-side | 🟡 |
| | Stage file listing (`LS @stage`, `@~`, `@%table`, `PATTERN`) | ✅ |
| | `REMOVE` / `RM @stage`, with subpath and `PATTERN` filters | ✅ |
| **Streams & Tasks** | Streams — `CREATE/DROP/SHOW STREAM`, DML change-record hooks | ✅ |
| | Tasks — `CREATE/DROP/ALTER/SHOW/EXECUTE TASK`, real cron parser, scheduler runs | ✅ |
| **Time Travel** | `UNDROP TABLE` — snapshot on `DROP TABLE`, restored via parquet | ✅ |
| | `AT (OFFSET => -N)` / `AT (TIMESTAMP => '…')` / `BEFORE (TIMESTAMP => …)` | ✅ |
| **Cloning** | Table / Schema / Database clone (CTAS-backed) | 🟡 |
| | Zero-copy snapshots (would need a storage-overlay DuckDB doesn't have) | 🚧 |
| **UDFs** | SQL UDFs | ✅ |
| | Python UDFs (subprocess-based) | 🟡 |
| | JavaScript UDFs (needs the `goja` runtime — opted out for now) | 🚧 |
| **RBAC** | Roles, grants — parsed, not enforced | ✅ |
| **Snowpipe** | `CREATE/DROP/SHOW PIPE` + REST `/v1/data/pipes/{db}/{schema}/{pipe}/insertFiles` | ✅ |
| **Internal** | `GET /_miniflake/health` | ✅ |
| | `POST /_miniflake/reset` (CI state wipe) | ✅ |
| **Information Schema** | `TABLES`, `COLUMNS`, `SCHEMATA` views | ✅ |

---

## Snowpipe REST ingest

The Snowpipe REST endpoint accepts the same shape Snowflake's API documents:

```bash
# Create a pipe via SQL (through your driver of choice)
CREATE PIPE my_pipe AS COPY INTO target_table FROM @my_stage;

# Trigger ingest via REST
curl -X POST http://localhost:8084/v1/data/pipes/TESTDB/PUBLIC/my_pipe/insertFiles \
  -H "Content-Type: application/json" \
  -d '{
    "files": [
      {"path": "2026/06/05/batch_1.csv", "size": 4096},
      {"path": "2026/06/05/batch_2.csv", "size": 8192}
    ]
  }'
# → {"requestId":"…","pipe":"TESTDB.PUBLIC.my_pipe","statusCode":200,"message":"SUCCESS"}
```

For each file, the pipe's COPY statement runs against the engine. `{FILE}` in the COPY statement is substituted with the file path.

---

## Architecture

```
Client (gosnowflake / Python / JDBC)
          |
          v
   +--------------+
   | HTTP Router  |  Snowflake-compatible REST API + Snowpipe REST
   +--------------+
          |
          v
   +--------------+
   | Orchestrator |  Routes Snowflake-specific syntax (USE, COPY, PUT,
   |              |  GET, LIST, REMOVE, CREATE STREAM/TASK/PIPE,
   |              |  AT/BEFORE, UNDROP, MERGE) to subsystem engines
   +--------------+
          |
          v
   +--------------+
   | SQL Rewriter |  Snowflake SQL -> DuckDB SQL (semi-structured access,
   |              |  function-name mappings, MERGE → INSERT … ON CONFLICT,
   |              |  LATERAL FLATTEN → LATERAL unnest)
   +--------------+
          |
          v
   +--------------+
   | DuckDB Engine|  Execution + storage
   +--------------+
          |
          v
     /data  /stages  /snapshots
```

In-process subsystems (each is a Go package under `internal/`):

| Package | Responsibility |
|---|---|
| `engine` | DuckDB connection + query/exec abstractions |
| `catalog` | Database/schema/table metadata |
| `stage` | Filesystem-backed Snowflake stages |
| `stream` | Table streams (CDC), change records, `SYSTEM$STREAM_HAS_DATA` |
| `task` | Task scheduler, real cron parser, DAG ordering |
| `timetravel` | Parquet snapshots, AT/BEFORE/UNDROP resolution |
| `clone` | CTAS-backed clone |
| `udf` | SQL + Python UDFs |
| `rbac` | GRANT/REVOKE parsing |
| `snowpipe` | Pipe metadata + REST insert |
| `session` | Auth tokens, session state |
| `rewriter` | Snowflake → DuckDB SQL translation |
| `server` | HTTP + wire-protocol layer |
| `orchestrator` | Routes statements to the right subsystem |

---

## Configuration

| Flag | Default | Description |
|---|---|---|
| `--host` | `0.0.0.0` | HTTP/HTTPS listen host |
| `--port` | `8084` | HTTP/HTTPS listen port (both are served on this one port) |
| `--data-dir` | `./data` | DuckDB database directory |
| `--stage-dir` | `./stages` | Internal stages directory |
| `--tls-cert` | (auto) | TLS certificate PEM. If unset, a self-signed cert is generated and persisted at `<data-dir>/miniflake-cert.pem`. |
| `--tls-key` | (auto) | TLS private key PEM. If unset, generated alongside the cert. |
| `--log-level` | `info` | Log level (debug, info, warn, error) |
| `--read-only` | `false` | Read-only mode |
| `--version` | — | Print version and exit |

---

## Development

Requires Go 1.26+ (the DuckDB binding is cgo-based, so a C toolchain is needed too — `build-essential` on Debian/Ubuntu, Xcode CLT on macOS).

```bash
make build              # Build binary at ./bin/miniflake
make test               # Unit tests with race detector
make test-integration   # Integration tests via gosnowflake (uses build tag `integration`)
make docker             # Build Docker image
make fmt                # gofmt + go vet
```

CI runs the same targets on every push and PR via `.github/workflows/test.yml` (Ubuntu + macOS, Go 1.26) plus a `lint` job (gofmt, vet, staticcheck). The `docker.yml` workflow builds the image (no push from PRs) and smoke-tests `/health`.

### Contributing

PRs welcome. For features that flip a 🟡 → ✅: add an integration test in `test/integration/miniflake_test.go` that exercises the feature through the gosnowflake driver before changing the README marker. The marker isn't an opinion — it's a load-bearing claim about test coverage.

---

## License

[MIT](LICENSE) — same as ministack. No telemetry, no account required, no paid tier.
