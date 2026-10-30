# Changelog

All notable changes to MiniFlake are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project
follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Bound query parameters.** `bindings` in `/queries/v1/query-request` and the
  SQL API v2 are now applied, so `?` and `:N` placeholders from JDBC, Python
  and gosnowflake work instead of failing with "incorrect argument count".
  Values are substituted as escaped, type-checked literals (TEXT, FIXED, REAL,
  BOOLEAN, DATE, TIME, TIMESTAMP_*, BINARY, NULL).
- **Bulk (array) bindings.** Array-valued binds, as sent by JDBC `executeBatch`
  and gosnowflake `Array(...)`, run the statement once per row. A plain
  `INSERT ... VALUES (...)` is folded into one multi-row INSERT (atomic); other
  DML runs row by row and is not atomic. Staged bulk binds (`bindStage`, used
  by drivers for very large batches) are not supported and now fail with a
  clear error instead of an "incorrect argument count" from DuckDB.

### Fixed
- **JDBC `addBatch()` / `executeBatch()` failed with "incorrect argument
  count".** `describeOnly` requests (sent by JDBC on `addBatch`, `getMetaData`)
  were executed, with no bind values. They now never run DML/DDL, and queries
  are described with placeholders set to NULL and no rows returned. Before this
  a describe of a parameterless INSERT would have inserted the row.
- **Query responses now carry `statementTypeId` (and `arrayBindSupported` for
  INSERT).** JDBC rejected `executeBatch` on the old "unknown" type, and
  gosnowflake reported `RowsAffected() == 0` for every DML statement.

## [0.2.0] - 2026-10-29

### Changed
- **DuckDB 2.0.0 LTS.** The engine moves from `github.com/marcboeker/go-duckdb`
  v1.8.5 (DuckDB 1.2) to `github.com/duckdb/duckdb-go/v2` v2.20000.0
  (DuckDB 2.0.0). **Storage format:** DuckDB 2.0 can still open existing
  `--data-dir` databases, but databases it creates use storage version 69,
  which DuckDB 1.x (MiniFlake ≤ 0.1.5) cannot read. Back up `--data-dir`
  before upgrading if you might need to roll back. The native libraries are
  now fetched from `go.duckdb.org`; builds behind a restricted `GOPROXY`
  must allow it.
- **The engine runs in the Snowflake default time zone.** Every DuckDB
  connection uses `America/Los_Angeles`, matching the `TIMEZONE` session
  default reported by `SHOW PARAMETERS` and the login response, instead of
  the host's local time zone.
- **`serverVersion` in the login response is now `10.37.0`** (was `8.0.0`).
- **Removed the `Makefile`.** CI, the release workflow and the Dockerfile
  never used it; README and CONTRIBUTING now list the plain `go` / `docker`
  commands. Local builds report version `dev`; release binaries are
  stamped by the release workflow from the tag.
- **Docker images report their release version.** The Dockerfile built
  without `-X main.version`, so every published image reported `dev`;
  `docker.yml` now passes the tag version and commit as build args.
- **Integration tests use `gosnowflake/v2` v2.3.0.** gosnowflake v1 is
  affected by GO-2026-6665 / CVE-2026-86597 (sensitive data written to
  logs), which has no v1 fix. Only the test suite depended on it.

### Fixed
- **`REGEXP_LIKE`, `RLIKE` and `REGEXP` now match the whole string.**
  Snowflake implicitly anchors the pattern at both ends, so `'abc' RLIKE 'b'`
  is `FALSE`. They were rewritten to DuckDB's `regexp_matches`, which
  matches anywhere in the string; they now use `regexp_full_match`. `NOT
  RLIKE` and quoted patterns containing spaces are rewritten correctly too.
- **Division by zero raises Snowflake's error.** `x / 0` returned `NULL`
  (DuckDB 1.x) and would have returned `inf` on DuckDB 2.0. It now fails
  with error `100051`, SQLSTATE `22012`, message `Division by zero`, like
  Snowflake. `DIV0` still returns 0.
- **`DIV0NULL` returns 0 for a zero or NULL divisor.** It returned `NULL`
  when the divisor was 0 and divided by `NULL` otherwise; Snowflake returns
  0 in both cases.
- **Login response now returns the default session parameters.** The
  `/session/v1/login-request` handler returned an empty `parameters` array,
  so clients that read session parameters from the login response got
  nothing back; for example, the JDBC driver unboxes `AUTOCOMMIT` as a
  Boolean and failed without it (#25). The response now carries the same
  default catalog as `SHOW PARAMETERS`, with native JSON types: booleans as
  `true`/`false`, numbers as integers and everything else as strings. Also
  adds the missing `CLIENT_RESULT_CHUNK_SIZE` (160) default. Contributed by
  @ivanmarban.

## [0.1.5] - 2026-08-10

### Fixed
- **Gzip-encoded request bodies are now decompressed.**
  `snowflake-connector-python` gzips every request body it sends,
  including the initial `login-request`, and sets `Content-Encoding:
  gzip`. The wire-protocol handlers passed `r.Body` straight to
  `json.Decode`, which fails on a gzip stream, so the Python connector was
  rejected with `400 "invalid request body"` and could never
  authenticate. A `withDecompressedBody` middleware now inflates the body
  when `Content-Encoding` selects gzip, covering every current handler and
  any route added later in one place. `gosnowflake` does not compress
  requests, which is why the integration suite never caught it. Contributed
  by @frazier-at-cpcc.
- **Query results are now readable by `snowflake-connector-python`.** The
  query response's `rowtype` carried only `name`, `type` and `nullable`.
  The Python connector's `ResultMetadata.from_column` reads `length`,
  `precision` and `scale` by direct key access, so the first `execute()`
  failed with `KeyError: 'length'`; `gosnowflake` unmarshals `rowtype` into
  an integer struct and tolerates the missing keys, which is why the
  integration suite never caught it. Each column now carries
  `length`/`precision`/`scale`/`byteLength` with Snowflake's per-type values
  (`NUMBER` reports precision/scale, timestamps report scale, `VARCHAR` and
  `BINARY` report length, others null). Booleans are also now wire-encoded
  as `1`/`0` rather than `true`/`false`: `gosnowflake` accepts both, but the
  Python connector decodes a boolean as true only for `1`/`TRUE`, so `true`
  silently read back as `False`. Verified end to end against
  `snowflake-connector-python` 4.7.2 (login, a mixed-type query, and a table
  round-trip).


## [0.1.4] - 2026-08-05

### Fixed
- **Login response now includes `sessionInfo` and `parameters`.** The
  `/session/v1/login-request` handler omitted both fields. `gosnowflake`
  and the Python connector tolerate their absence, but the Snowflake .NET
  connector dereferences `data.sessionInfo` and iterates `data.parameters`
  unconditionally in `ProcessLoginResponse`, so login panicked with a
  `NullReferenceException` ("Object reference not set to an instance of an
  object") before the connection opened. The response now always carries a
  populated `sessionInfo` (database/schema/warehouse/role) and a non-nil
  `parameters` array, unblocking the .NET connector over HTTPS. Contributed
  by @SaiSDET.

## [0.1.3] - 2026-08-04

### Added
- **`SHOW PARAMETERS`.** Returns a default session parameter catalog
  (`key`, `value`, `default`, `level`, `description`, `type`) so drivers
  that probe config at connect time get a real result set. Supports
  `LIKE` and `IN SESSION` / `IN ACCOUNT`. `ALTER SESSION` is not wired
  yet, so value always matches default. Contributed by @fedemeister.

### Changed
- **`CREATE TASK` validates cron timezones.** Invalid IANA zones in
  `USING CRON ... <tz>` fail at create time. The timezone stays inside
  the `schedule` string (Snowflake has no separate `timezone` column on
  `SHOW TASKS`). Contributed by @fedemeister.
- **`POST /_miniflake/reset`.** Wipes DuckDB user tables, views and schemas,
  detaches non-primary databases, clears stages on disk, sessions, and
  in-process subsystem state (tasks, streams, pipes, UDFs, RBAC,
  time-travel snapshots) so CI can isolate runs without restarting the
  process. Same idea as ministack's `/_ministack/reset`: an exclusive
  lock blocks other requests while the wipe runs; health stays unlocked
  for liveness checks. Catalog state matches a fresh boot
  (`Catalog.Init` only). Attached `.duckdb` files on disk are left in
  place (DETACH only). Contributed by @fedemeister.

## [0.1.2] - 2026-08-03

### Added
- **`REMOVE @stage` / `RM @stage`.** Deletes staged files for named,
  user (`@~`) and table (`@%table`) stages, honouring subpath and
  `PATTERN` filters, and returning Snowflake's `name, result` columns.
  Contributed by @fedemeister.
- **`LIST @stage` / `LS @stage`.** Server-side file listing for named,
  user (`@~`), and table (`@%table`) stages, including qualified refs
  (`@db.schema.stage`), subpath filters (`@stage/path`), and
  `PATTERN = '<regex>'`. Wrapped by the rewriter in a marker and answered
  from the stage manager (DuckDB has no `@stage` concept), the same
  routing PUT/GET/COPY INTO use. Returns `name, size, md5, last_modified`
  matching real Snowflake output.

  `PATTERN` matches against the whole relative path, as Snowflake does,
  so `'a[.]csv'` does not match `dir/a.csv`; lead with `.*` to match
  anywhere. MD5 is computed only for `LIST`/`LS`, keeping the
  `ListFiles` path used by `COPY INTO` / `GET` free of hashing. Stages
  whose backing directory is missing, and files removed concurrently,
  list as absent instead of failing the statement. Contributed by
  @fedemeister.

### Fixed
- **`CREATE STAGE IF NOT EXISTS` and `CREATE OR REPLACE STAGE`.** Both
  clauses were parsed and then ignored, so either form failed once the
  stage existed and no setup script could be re-run. `OR REPLACE` now
  recreates the stage and discards its files, as in Snowflake.
- **`COPY INTO` from an unqualified stage.** `COPY INTO t FROM @my_stage`
  built its stage key from empty database and schema parts, looking up
  `..MY_STAGE` and always failing with `stage does not exist`. Only the
  fully qualified `@db.schema.stage` form worked, which is why the
  existing tests (all qualified) never caught it; the form documented in
  this README's own Snowpipe example could not run. Stage references are
  now resolved in one place for every statement that takes one, so
  `COPY INTO`, `LIST`, `REMOVE`, `PUT` and `GET` agree on what a
  reference means. `PUT` and `GET` gain qualified, `@~` and `@%table`
  references as a result.Contributed by @fedemeister.
- **Stage paths can no longer escape their stage.** A reference such as
  `@s/../../etc` addressed files outside the stage directory, because
  `filepath.Join` cleans `../` segments instead of rejecting them. The
  containment check (`stage.ResolveInStage`) is now shared by every
  statement that writes through a stage path — `PUT`, `GET`, `REMOVE` and
  `COPY INTO` unload — closing the remaining `COPY INTO @s/../../x` write
  escape.Contributed by @fedemeister.
- **`@stage/<path>` is a literal prefix.** Subpath filtering matched whole
  path components only; Snowflake treats the path as a raw string prefix
  ("names that begin with a common string"), so `@stage/data` now also
  matches `database.csv`, matching real Snowflake. Contributed by
  @fedemeister.

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

[0.2.0]: https://github.com/ministackorg/miniflake/compare/v0.1.5...v0.2.0
[0.1.5]: https://github.com/ministackorg/miniflake/compare/v0.1.4...v0.1.5
[0.1.4]: https://github.com/ministackorg/miniflake/compare/v0.1.3...v0.1.4
[0.1.3]: https://github.com/ministackorg/miniflake/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/ministackorg/miniflake/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/ministackorg/miniflake/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/ministackorg/miniflake/releases/tag/v0.1.0
