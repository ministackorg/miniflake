package orchestrator

import (
	"fmt"
	"regexp"

	"github.com/miniflakedb/miniflake/internal/session"
	"github.com/miniflakedb/miniflake/internal/stage"
)

// LIST / LS @stage is intercepted at the top of handleQuery, ahead of its
// generic "starts with LIST" bucket, which would otherwise forward the
// statement to DuckDB (which knows nothing about @stage syntax). See
// stageref.go for the reference forms accepted after the "@".
//
// Optional: PATTERN = '<regex>' (RE2; Snowflake documents Java Pattern).
var (
	reListStage   = regexp.MustCompile(`(?i)^(?:LIST|LS)\s+@(\S+)`)
	reListPattern = regexp.MustCompile(`(?i)\bPATTERN\s*=\s*'([^']*)'`)
)

// listTimeFormat matches the last_modified rendering of real Snowflake's LIST
// output. "GMT" is a literal here, not a layout element; net/http relies on
// the same trick for http.TimeFormat.
const listTimeFormat = "Mon, 02 Jan 2006 15:04:05 GMT"

func (o *Orchestrator) handleListStage(sess *session.Session, sql string) (*QueryResult, bool, error) {
	m := reListStage.FindStringSubmatch(sql)
	if m == nil {
		return nil, false, nil
	}

	meta, subpath, namePrefix, err := o.resolveStage(sess, m[1])
	if err != nil {
		return nil, true, fmt.Errorf("LIST: %w", err)
	}

	pattern := ""
	if pm := reListPattern.FindStringSubmatch(sql); pm != nil {
		pattern = pm[1]
	}

	files, err := o.stageMgr.ListMetaFiles(meta, stage.ListOptions{
		Prefix:   subpath,
		Regex:    pattern,
		Checksum: true,
	})
	if err != nil {
		return nil, true, err
	}

	rows := make([][]interface{}, 0, len(files))
	for _, f := range files {
		rows = append(rows, []interface{}{
			namePrefix + "/" + f.Name,
			f.Size,
			f.MD5,
			f.ModTime.UTC().Format(listTimeFormat),
		})
	}
	return &QueryResult{
		Columns:       []string{"name", "size", "md5", "last_modified"},
		Rows:          rows,
		StatementType: "LIST",
	}, true, nil
}
