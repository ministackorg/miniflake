package orchestrator

import (
	"fmt"
	"regexp"

	"github.com/miniflakedb/miniflake/internal/session"
	"github.com/miniflakedb/miniflake/internal/stage"
)

// REMOVE / RM @stage deletes staged files. Like LIST it returns a result set
// and resolves its reference through stageref.go, so `REMOVE @s/path` and
// `LIST @s/path` always address the same files. It reuses reListPattern for
// the PATTERN filter, which Snowflake accepts on both statements.
var reRemoveStage = regexp.MustCompile(`(?i)^(?:REMOVE|RM)\s+@(\S+)`)

func (o *Orchestrator) handleRemoveStage(sess *session.Session, sql string) (*QueryResult, bool, error) {
	m := reRemoveStage.FindStringSubmatch(sql)
	if m == nil {
		return nil, false, nil
	}

	meta, subpath, namePrefix, err := o.resolveStage(sess, m[1])
	if err != nil {
		return nil, true, fmt.Errorf("REMOVE: %w", err)
	}

	pattern := ""
	if pm := reListPattern.FindStringSubmatch(sql); pm != nil {
		pattern = pm[1]
	}

	files, err := o.stageMgr.ListMetaFiles(meta, stage.ListOptions{
		Prefix: subpath,
		Regex:  pattern,
	})
	if err != nil {
		return nil, true, fmt.Errorf("REMOVE: %w", err)
	}

	rows := make([][]interface{}, 0, len(files))
	for _, f := range files {
		result := "removed"
		if rmErr := o.stageMgr.RemoveMetaFile(meta, f.Name); rmErr != nil {
			result = "failed: " + rmErr.Error()
		}
		rows = append(rows, []interface{}{namePrefix + "/" + f.Name, result})
	}
	return &QueryResult{
		Columns:       []string{"name", "result"},
		Rows:          rows,
		StatementType: "REMOVE",
	}, true, nil
}
