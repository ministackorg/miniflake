// Command miniflake is the local Snowflake-compatible HTTP server.
//
// It wires together the full subsystem stack so the binary matches what the
// integration tests exercise: engine → orchestrator → server, with streams,
// tasks (started via the scheduler), time-travel snapshots, clones, UDFs,
// RBAC parsing, snowpipe, and stages all attached.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/miniflakedb/miniflake/internal/catalog"
	"github.com/miniflakedb/miniflake/internal/clone"
	"github.com/miniflakedb/miniflake/internal/engine"
	"github.com/miniflakedb/miniflake/internal/orchestrator"
	"github.com/miniflakedb/miniflake/internal/rbac"
	"github.com/miniflakedb/miniflake/internal/server"
	"github.com/miniflakedb/miniflake/internal/session"
	"github.com/miniflakedb/miniflake/internal/snowpipe"
	"github.com/miniflakedb/miniflake/internal/stage"
	"github.com/miniflakedb/miniflake/internal/stream"
	"github.com/miniflakedb/miniflake/internal/task"
	"github.com/miniflakedb/miniflake/internal/timetravel"
	"github.com/miniflakedb/miniflake/internal/udf"
)

// Build-time variables (populated by Makefile via -ldflags).
var (
	version = "dev"
	commit  = "unknown"
)

func main() {
	var (
		host        = flag.String("host", "0.0.0.0", "HTTP listen host")
		port        = flag.Int("port", 8084, "HTTP/HTTPS listen port")
		dataDir     = flag.String("data-dir", "./data", "DuckDB database directory")
		stageDir    = flag.String("stage-dir", "./stages", "Internal stages directory")
		tlsCert     = flag.String("tls-cert", "", "TLS certificate PEM (auto-generated self-signed if unset)")
		tlsKey      = flag.String("tls-key", "", "TLS private key PEM (auto-generated self-signed if unset)")
		logLevel    = flag.String("log-level", "info", "Log level (debug, info, warn, error)")
		readOnly    = flag.Bool("read-only", false, "Read-only mode")
		showVersion = flag.Bool("version", false, "Print version and exit")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("miniflake %s (%s)\n", version, commit)
		return
	}

	// log-level / read-only are accepted for parity with the documented
	// flags but not yet plumbed through the subsystems.
	_ = *logLevel
	_ = *readOnly

	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		log.Fatalf("create data dir %s: %v", *dataDir, err)
	}
	if err := os.MkdirAll(*stageDir, 0o755); err != nil {
		log.Fatalf("create stage dir %s: %v", *stageDir, err)
	}

	// Engine first — every other subsystem either holds a reference to it
	// or executes through it.
	eng, err := engine.New(*dataDir)
	if err != nil {
		log.Fatalf("init engine: %v", err)
	}
	defer eng.Close()

	// Catalog seeds the default database/schema and tracks DDL.
	cat := catalog.New()
	cat.Init()

	stageMgr := stage.NewManager(*stageDir)
	streamEng := stream.NewEngine()
	ttEng := timetravel.NewEngine(filepath.Join(*dataDir, "snapshots"), 7*24*time.Hour)
	udfReg := udf.NewRegistry()
	rbacEng := rbac.NewEngine()
	rbacEng.Init()

	snowpipeEng := snowpipe.NewEngine(func(ctx context.Context, sql string) error {
		_, err := eng.ExecNoResult(ctx, sql)
		return err
	})

	cloneExec := func(ctx context.Context, sql string) error {
		_, err := eng.ExecNoResult(ctx, sql)
		return err
	}
	cloneQuery := func(ctx context.Context, sql string) ([]string, [][]interface{}, error) {
		return eng.Execute(ctx, sql)
	}
	cloneEng := clone.NewEngine(cloneExec, cloneQuery)

	taskExec := func(ctx context.Context, sql string) error {
		_, err := eng.ExecNoResult(ctx, sql)
		return err
	}
	taskSched := task.NewScheduler(taskExec)
	taskSched.SetStreamHasDataFn(func(db, schema, name string) bool {
		return streamEng.HasData(db, schema, name)
	})

	orch := orchestrator.New(
		eng, cat, stageMgr, streamEng, taskSched,
		ttEng, cloneEng, udfReg, rbacEng, snowpipeEng,
	)

	// Background scheduler — tasks scheduled via `CREATE TASK ... SCHEDULE
	// = '...'` fire on this loop. Stopped on shutdown.
	schedCtx, schedCancel := context.WithCancel(context.Background())
	defer schedCancel()
	taskSched.Start(schedCtx)
	defer taskSched.Stop()

	sessMgr := session.NewManager()
	defer sessMgr.Stop()

	srv := server.New(eng, sessMgr, *host, *port, *stageDir, orch)

	// Serve HTTPS and plain HTTP on the same port. Drivers that require TLS
	// (e.g. the Snowflake .NET connector, which has no plain-HTTP mode) connect
	// over https; gosnowflake/Python/JDBC clients using protocol=http are
	// unaffected. A self-signed cert is generated once and persisted under the
	// data dir unless --tls-cert/--tls-key are supplied.
	if err := srv.EnableTLS(*tlsCert, *tlsKey, *dataDir); err != nil {
		log.Fatalf("enable tls: %v", err)
	}

	// Graceful shutdown on SIGINT / SIGTERM.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	errCh := make(chan error, 1)
	go func() {
		log.Printf("miniflake %s listening on %s:%d (https+http, cert=%s, data=%s stages=%s)",
			version, *host, *port, srv.CertPath(), *dataDir, *stageDir)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if err != nil {
			log.Fatalf("server error: %v", err)
		}
	case sig := <-stop:
		log.Printf("received %s — shutting down", sig)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			log.Printf("shutdown error: %v", err)
		}
	}
}
