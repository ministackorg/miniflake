# Changelog

All notable changes to MiniFlake are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project
follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **`LIST @stage` / `LS @stage`.** Server-side file listing for named,
  user (`@~`), and table (`@%table`) stages, including qualified refs
  (`@db.schema.stage`), subpath filters (`@stage/path`), and
  `PATTERN = '<regex>'`. Routed before the generic query path (which
  previously forwarded `LIST` straight to DuckDB and broke on `@stage`
  syntax). Returns `name, size, md5, last_modified` matching real
  Snowflake output.

  `PATTERN` matches against the whole relative path, as Snowflake does,
  so `'a[.]csv'` does not match `dir/a.csv`; lead with `.*` to match
  anywhere. MD5 is computed only for `LIST`/`LS`, keeping the
  `ListFiles` path used by `COPY INTO` / `GET` free of hashing. Stages
  whose backing directory is missing, and files removed concurrently,
  list as absent instead of failing the statement.

## [0.1.1] - 2026-07-29

### Added
- **HTTPS support.** MiniFlake now serves TLS and plain HTTP on the same
  `--port`; the first byte of each connection selects the protocol. Drivers
  that require TLS and have no plain-HTTP mode — notably the Snowflake .NET
  connector — can now connect, while `protocol=http` clients (gosnowflake,
  Python, JDBC) are unaffected. A self-signed certificate is generated once and
  persisted at `<data-dir>/miniflake-cert.pem` (stable across restarts, so
  strict clients trust it once); `--tls-cert` / `--tls-key` override it with
  your own key pair. Reported by @SaiSDET.

## [0.1.0] - 2026-06-06

First public release. MiniFlake serves the Snowflake HTTP wire protocol on
top of DuckDB. The query path is exercised end-to-end through the official
gosnowflake driver in `test/integration/`.

### Added

- **Build**: `cmd/miniflake/main.go` entrypoint, wires the full subsystem
  stack (engine → orchestrator → server) with the task scheduler started.
  `--host`, `--port`, `--data-dir`, `--stage-dir`, `--log-level`,
  `--read-only`, `--version` flags.
- **MERGE INTO** rewriter (single-key upsert form) — maps to DuckDB
  `INSERT ... ON CONFLICT DO UPDATE`. Complex MERGE forms pass through.
- **Streams (CDC)**: `CREATE/DROP/SHOW STREAM`, DML change-record hooks,
  `SYSTEM$STREAM_HAS_DATA` integration with the task scheduler.
- **Tasks**: `CREATE/DROP/ALTER/SHOW/EXECUTE TASK`, real stdlib cron
  parser (lists, ranges, steps, IANA timezone suffix, Vixie-cron OR
  rule), DAG ordering.
- **Pipes**: `CREATE/DROP/SHOW PIPE` and Snowpipe REST ingest at
  `POST /v1/data/pipes/{db}/{schema}/{pipe}/insertFiles`.
- **Time travel**: `UNDROP TABLE` (snapshot on `DROP`, restored via
  parquet) and `AT (OFFSET => -N) / AT (TIMESTAMP => '…') / BEFORE`
  query rewriting via pre-DML snapshot capture.
- **PUT / GET** server-side handlers (file transfer via the stage
  manager). The gosnowflake driver intercepts these client-side for its
  own presigned-URL flow; the Python connector and JDBC reach the
  server.
- **`LATERAL FLATTEN`** rewrite that binds the value column so `t.value`
  resolves through DuckDB's `unnest`.
- **CI**: `.github/workflows/test.yml` (Ubuntu + macOS, Go 1.26, race
  tests, gofmt, vet, staticcheck) and `.github/workflows/docker.yml`
  (PR smoke-test, main + tag publish to GHCR).
- **Honest README**: feature matrix with three markers — ✅ exercised by
  integration test, 🟡 building block exists but not wired end-to-end,
  🚧 planned.

### Fixed

- Server: invalid session tokens no longer fall through to a temporary
  session. Return HTTP 401 with code `390100`.
- Server: `SessionID` derived from an atomic counter (was
  `time.UnixMilli()%100000`, which collided under concurrent login).
- Server: `handleLogin` reads `databaseName` / `schemaName` /
  `warehouse` from URL query parameters in addition to the body —
  gosnowflake passes them in the query string and earlier sessions
  defaulted everything to `miniflake`/`main`.
- Session: empty database/schema default to `miniflake`/`main`
  consistently for downstream multi-tenant lookups.
- `internal/copyinto/fileformat.go`: pre-existing map-iteration race
  where `TYPE = CSV` could reset previously-iterated options (e.g.
  `SKIP_HEADER`). Now done in two passes (TYPE first).
- `internal/timetravel/timetravel.go`: `CaptureSnapshot` no longer
  qualifies the COPY statement with `"db"."schema"."table"` — DuckDB
  only knows tables under the current schema, the prefix was causing
  every snapshot capture to fail silently.
- `internal/task/task.go`: dropped unused `allNames` accumulator
  (staticcheck SA4010); switched lowercased equality compares to
  `strings.EqualFold` (SA6005).

### Known limitations

- **PUT / GET** through `gosnowflake` won't reach the server — the
  driver intercepts these and runs its own presigned-URL flow. Python
  and JDBC clients reach the server-side handler.
- **Cloning** is CTAS-backed, not true zero-copy. Zero-copy snapshots
  would require a storage overlay DuckDB doesn't natively support.
- **JavaScript UDFs** are not implemented (would need the `goja`
  runtime, intentionally deferred).
- **`AT (OFFSET => -N)` for very fresh tables**: a snapshot exists but
  the cutoff is more recent than every snapshot, so the query returns
  "no snapshot at or before". Matches the Snowflake semantic.

[Unreleased]: https://github.com/ministackorg/miniflake/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/ministackorg/miniflake/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/ministackorg/miniflake/releases/tag/v0.1.0
