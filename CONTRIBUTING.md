# Contributing to MiniFlake

Thanks for wanting to contribute. The codebase is organised by subsystem
under `internal/`, with a thin HTTP layer that speaks the Snowflake wire
protocol and a SQL rewriter that translates Snowflake-specific syntax to
DuckDB. Most fixes and features land in 1-3 files plus a test.

## Project Structure

```
miniflake/
├── cmd/miniflake/main.go         # Entrypoint; wires subsystems
├── internal/
│   ├── server/                   # HTTP wire-protocol layer
│   ├── orchestrator/             # Routes statements to subsystems
│   ├── rewriter/                 # Snowflake SQL → DuckDB SQL
│   ├── engine/                   # DuckDB connection + query/exec
│   ├── catalog/                  # Database/schema/table metadata
│   ├── session/                  # Auth tokens, session state
│   ├── stage/                    # Filesystem-backed stages
│   ├── stream/                   # Streams (CDC)
│   ├── task/                     # Tasks + cron parser
│   ├── timetravel/               # Parquet snapshots, AT/BEFORE/UNDROP
│   ├── clone/                    # CTAS-backed clone
│   ├── udf/                      # SQL + Python UDFs
│   ├── rbac/                     # GRANT/REVOKE parsing
│   ├── snowpipe/                 # Pipe metadata + REST insert
│   └── copyinto/                 # COPY INTO parser + executor
├── test/integration/             # gosnowflake-driven end-to-end tests
├── Dockerfile
├── go.mod / go.sum
├── Makefile
├── CHANGELOG.md
└── .github/
    ├── workflows/                # test, docker, release
    ├── ISSUE_TEMPLATE/
    └── PULL_REQUEST_TEMPLATE.md
```

---

## For infrastructure changes (Dockerfile, CI, Makefile, dependencies), open an issue first.

PRs changing CI, the Dockerfile, the release flow, or the dependency graph
without a prior issue will be asked to back up and discuss the design.

## FOR NEW FEATURES — Open an Issue First

> This section applies when you're adding a brand-new Snowflake feature
> surface (a new SQL command, a new endpoint, a new subsystem under
> `internal/`).

**Before writing code for a new Snowflake feature, open a GitHub issue.**
Use the `enhancement` label and describe:

1. **Which Snowflake feature** — full SQL syntax or REST endpoint shape,
   linked to the Snowflake docs page.
2. **Which sub-operations** you actually need. We favour the operations
   real users hit (the ones that show up in Terraform / DBT / production
   queries) over breadth.
3. **A real use case** — what tool, framework, or workflow drove the
   need. "I want full parity" is not a use case.
4. **Scope boundaries** — what's explicitly out of scope for the first PR
   (e.g. "no column masking, no row access policies, no replication" for
   a first cut of Dynamic Tables). It's fine to ship a partial feature;
   it's not fine to ship a 10-statement stub where 9 return errors.

A maintainer will confirm scope, flag overlap with in-flight work, and
point you at the right layer (rewriter vs. orchestrator vs. server)
before you write code. This avoids large PRs that get rejected for scope
drift or rebased away by parallel work.

**PRs that add a new Snowflake feature without a corresponding scoped
issue will be closed and the contributor asked to open one.**

---

## The Three-Layer Mental Model

A Snowflake-specific statement reaches MiniFlake in one of three shapes
and you need to know which:

1. **Wire-protocol shape** (login, query envelope, fetch) — handled in
   `internal/server/`. New routes go here.
2. **SQL that DuckDB can execute directly with a small rewrite** —
   handled in `internal/rewriter/`. Examples: `IFF`, `NVL`, dot/bracket
   semi-structured access, `LATERAL FLATTEN`.
3. **SQL that DuckDB has no analogue for** — handled in
   `internal/orchestrator/` via a marker comment the rewriter emits, with
   the actual implementation in a subsystem under `internal/`. Examples:
   `CREATE STREAM`, `CREATE TASK`, `AT/BEFORE`, `MERGE` (upsert form).

Pick the right layer first. Putting orchestrator-level state in the
rewriter, or putting a regex-translatable function in the orchestrator,
will get the PR sent back to the drawing board.

---

## Adding a New SQL Rewrite Rule

```go
// In internal/rewriter/<topic>.go (add a new file or extend an existing
// one — one rule per regex)

var reMyFunction = regexp.MustCompile(`(?i)\bMY_FUNCTION\s*\(`)

func rewriteMyFunction(sql string) string {
    return replaceFuncCall(sql, "MY_FUNCTION", func(args []string) string {
        // Translate to DuckDB syntax
        return "duckdb_equivalent(" + strings.Join(args, ", ") + ")"
    })
}
```

Register the rule in `internal/rewriter/rewriter.go`'s `init()`. Add a
table-driven test case to `internal/rewriter/rewriter_test.go`.

---

## Adding a New Orchestrator-Routed Feature

1. **Rewriter side** — add a marker rewriter in
   `internal/rewriter/snowflake_ddl.go`:
   ```go
   var reCreateFoo = regexp.MustCompile(`(?is)^\s*CREATE\s+FOO\b`)

   func rewriteFoo(sql string) string {
       trimmed := strings.TrimRight(strings.TrimSpace(sql), ";")
       if reCreateFoo.MatchString(trimmed) {
           return "/* MINIFLAKE_CREATE_FOO " + trimmed + " */"
       }
       return sql
   }
   ```
   Register in `rules` and add a `*_test.go` case.

2. **Orchestrator side** — add a marker regex + handler in
   `internal/orchestrator/snowflake_handlers.go`:
   ```go
   var reMarkerCreateFoo = regexp.MustCompile(`(?s)/\*\s*MINIFLAKE_CREATE_FOO\s+(.*?)\s*\*/`)

   func (o *Orchestrator) handleCreateFoo(sess *session.Session, originalSQL string) (*QueryResult, bool, error) {
       // Parse, dispatch to subsystem, return result
   }
   ```
   Wire it into `handleSpecialMarkers` in `orchestrator.go`.

3. **Subsystem side** — implement the actual state and behavior in
   `internal/foo/foo.go` if it's a new subsystem.

4. **Integration test** — add a `gosnowflake`-driven test in
   `test/integration/miniflake_test.go`. The marker in the feature
   matrix doesn't flip from 🟡 to ✅ without an integration test.

---

## Running Tests Locally

Requires Go 1.26+ and a C toolchain (DuckDB binding is cgo).

```bash
# Unit tests with race detector
make test

# Integration tests (build tag `integration`, gosnowflake driver)
make test-integration

# Both
make test && make test-integration

# Single test
go test -race -run TestMergeUpsert -tags=integration ./test/integration/

# Format + vet
make fmt
```

CI runs the same commands in `.github/workflows/test.yml`.

---

## Code Conventions

- **Subsystem packages** are self-contained — `internal/<thing>/` holds
  state, public API, and tests. Cross-package state lives in the
  orchestrator's struct.
- **Public functions** return `error` last. Internal helpers can panic
  for invariant violations (caller bug, not data bug).
- **In-memory state** lives behind a `sync.RWMutex`. Mutation paths take
  the write lock; read paths take the read lock.
- **Reset semantics** — every subsystem that holds state exposes a
  `Reset()` or equivalent for test isolation. (Not yet wired into a
  `/_miniflake/reset` HTTP endpoint — that's a known gap; see
  `CHANGELOG.md`.)
- **No external Snowflake deps** — no `snowflake-connector-*` in the
  service code. The `gosnowflake` package is allowed in
  `test/integration/` only.
- **Logging** — `logger := logger.With(zap.String("subsystem", "foo"))`.
  DEBUG for per-request detail, INFO for lifecycle events. Avoid
  `fmt.Println` in production code (we strip these in code review).
- **Error responses** — match real Snowflake error codes and HTTP
  status codes where they're documented. Use code `002043` for generic
  exec failures (matches gosnowflake's `ExecContext` expectation).
- **gofmt + go vet + staticcheck must be clean.**

---

## Pull Request Checklist

- [ ] New code in the right layer (rewriter / orchestrator / subsystem)
- [ ] Unit tests for any new function, exercised path, or regex
- [ ] Integration test in `test/integration/` if the feature is
      user-visible through SQL
- [ ] Feature matrix in `README.md` updated (move 🟡 → ✅ only with an
      integration test backing the claim)
- [ ] `CHANGELOG.md` entry under `## [Unreleased]`
- [ ] `make test && make test-integration` green locally
- [ ] `gofmt -w .` and `go vet ./...` clean
- [ ] `staticcheck ./...` clean (or noisy items justified in the PR
      description)

---

## What We're Looking For

High-value contributions right now (state of the project as of v0.1.0):

- **`AT (STATEMENT => 'id')`** — statement-id-keyed time travel; today
  only OFFSET and TIMESTAMP work.
- **A `/_miniflake/reset` HTTP endpoint** for CI isolation, mirroring
  ministack's pattern.
- **More `Information Schema` views** (`TABLE_PRIVILEGES`,
  `ROLE_GRANTS`).

---

## Questions?

Open a GitHub Discussion or file an issue with the `question` label.
