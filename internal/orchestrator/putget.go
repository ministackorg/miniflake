package orchestrator

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/miniflakedb/miniflake/internal/session"
	"github.com/miniflakedb/miniflake/internal/stage"
)

// PUT and GET file transfers. Real Snowflake's PUT/GET protocol uses
// presigned URLs the driver writes to directly; for the local emulator we
// rely on the client and server sharing a filesystem (always true in
// dev/CI/Docker-mounted contexts) and let the stage manager do the copy
// in-process.
//
// Snowflake's gosnowflake driver expects a SQL-shaped response with the
// file-list columns it surfaces to callers. We return those columns even
// though we never round-tripped through the presigned-URL flow.

// PUT file:///<path> @<stage>[/<dest>] [<options>]
var rePutStmt = regexp.MustCompile(`(?is)^\s*PUT\s+(\S+)\s+@(\S+)`)

// GET @<stage>[/<src>] file:///<localdir> [<options>]
var reGetStmt = regexp.MustCompile(`(?is)^\s*GET\s+@(\S+)\s+(\S+)`)

func (o *Orchestrator) handlePut(sess *session.Session, putSQL string) (*QueryResult, bool, error) {
	m := rePutStmt.FindStringSubmatch(putSQL)
	if m == nil {
		return nil, true, fmt.Errorf("PUT: unable to parse %q", putSQL)
	}
	sourceURL := strings.Trim(m[1], `"'`)
	stagePath := strings.Trim(m[2], `"'`)

	localPath, err := fileURLToPath(sourceURL)
	if err != nil {
		return nil, true, fmt.Errorf("PUT: %w", err)
	}

	meta, destSubPath, _, err := o.resolveStage(sess, stagePath)
	if err != nil {
		return nil, true, fmt.Errorf("PUT: %w", err)
	}
	destName := filepath.Base(localPath)
	if destSubPath != "" {
		destName = destSubPath + "/" + destName
	}

	info, statErr := os.Stat(localPath)
	if statErr != nil {
		return nil, true, fmt.Errorf("PUT: stat %s: %w", localPath, statErr)
	}
	sourceSize := info.Size()

	if err := o.stageMgr.PutMetaFile(meta, localPath, destName); err != nil {
		return &QueryResult{
			Columns: []string{
				"source", "target", "source_size", "target_size",
				"source_compression", "target_compression", "status", "encryption", "message",
			},
			Rows: [][]interface{}{{
				filepath.Base(localPath), destName, sourceSize, int64(0),
				"NONE", "NONE", "ERROR", "AUTO", err.Error(),
			}},
			StatementType: "PUT",
		}, true, nil
	}

	return &QueryResult{
		Columns: []string{
			"source", "target", "source_size", "target_size",
			"source_compression", "target_compression", "status", "encryption", "message",
		},
		Rows: [][]interface{}{{
			filepath.Base(localPath), destName, sourceSize, sourceSize,
			"NONE", "NONE", "UPLOADED", "AUTO", "",
		}},
		StatementType: "PUT",
	}, true, nil
}

func (o *Orchestrator) handleGet(sess *session.Session, getSQL string) (*QueryResult, bool, error) {
	m := reGetStmt.FindStringSubmatch(getSQL)
	if m == nil {
		return nil, true, fmt.Errorf("GET: unable to parse %q", getSQL)
	}
	stagePath := strings.Trim(m[1], `"'`)
	destURL := strings.Trim(m[2], `"'`)

	localDir, err := fileURLToPath(destURL)
	if err != nil {
		return nil, true, fmt.Errorf("GET: %w", err)
	}

	meta, srcSubPath, _, err := o.resolveStage(sess, stagePath)
	if err != nil {
		return nil, true, fmt.Errorf("GET: %w", err)
	}

	// Snowflake GET returns every file under the stage path. When the source
	// path is empty, all files in the stage are returned.
	files, err := o.stageMgr.ListMetaFiles(meta, stage.ListOptions{Prefix: srcSubPath})
	if err != nil {
		return nil, true, fmt.Errorf("GET: %w", err)
	}
	if err := os.MkdirAll(localDir, 0o755); err != nil {
		return nil, true, fmt.Errorf("GET: create local dir %s: %w", localDir, err)
	}

	rows := make([][]interface{}, 0, len(files))
	for _, f := range files {
		base := filepath.Base(f.Name)
		localPath := filepath.Join(localDir, base)
		if err := o.stageMgr.GetMetaFile(meta, f.Name, localPath); err != nil {
			rows = append(rows, []interface{}{
				f.Name, int64(0), "NONE", "ERROR", err.Error(),
			})
			continue
		}
		rows = append(rows, []interface{}{
			localPath, f.Size, "NONE", "DOWNLOADED", "",
		})
	}
	return &QueryResult{
		Columns:       []string{"file", "size", "status_code", "status", "message"},
		Rows:          rows,
		StatementType: "GET",
	}, true, nil
}

// fileURLToPath converts a file:// URL to a local path. Bare paths are
// accepted as-is so a user can write `PUT /tmp/data.csv @s` instead of the
// longer URL form.
func fileURLToPath(u string) (string, error) {
	if !strings.HasPrefix(u, "file://") {
		if filepath.IsAbs(u) {
			return u, nil
		}
		return "", fmt.Errorf("file URL or absolute path required, got %q", u)
	}
	parsed, err := url.Parse(u)
	if err != nil {
		return "", err
	}
	return parsed.Path, nil
}
