package orchestrator

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/miniflakedb/miniflake/internal/session"
	"github.com/miniflakedb/miniflake/internal/stage"
)

// LIST / LS @stage is intercepted at the top of handleQuery, ahead of its
// generic "starts with LIST" bucket, which would otherwise forward the
// statement to DuckDB (which knows nothing about @stage syntax).
//
// Supported forms (Snowflake internalStage):
//
//	@[db.][schema.]stage[/path]
//	@[db.][schema.]%table[/path]
//	@~[/path]
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

	ref, subpath, err := splitStageRef(cleanIdent(m[1]))
	if err != nil {
		return nil, true, err
	}

	meta, namePrefix, err := o.resolveStageRef(sess, ref)
	if err != nil {
		return nil, true, err
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

// splitStageRef separates "@ref[/sub/path]" into (ref, subpath).
func splitStageRef(raw string) (ref, subpath string, err error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", fmt.Errorf("LIST: empty stage reference")
	}
	if idx := strings.Index(raw, "/"); idx != -1 {
		return raw[:idx], strings.TrimPrefix(raw[idx+1:], "/"), nil
	}
	return raw, "", nil
}

// resolveStageRef resolves a stage token (no leading @, no subpath) against the
// session, including user (@~) and table (@%t) stages.
func (o *Orchestrator) resolveStageRef(sess *session.Session, ref string) (*stage.StageMeta, string, error) {
	switch {
	case ref == "~":
		user := sess.User
		if user == "" {
			user = "DEFAULT"
		}
		return o.stageMgr.GetUserStage(user), "~", nil

	case strings.HasPrefix(ref, "%"):
		db, schema, table := qualifyIdent(strings.TrimPrefix(ref, "%"), sess)
		if table == "" {
			return nil, "", fmt.Errorf("LIST: empty table stage reference")
		}
		return o.stageMgr.GetTableStage(db, schema, table), "%" + strings.ToLower(table), nil

	default:
		db, schema, name := qualifyIdent(ref, sess)
		if name == "" {
			return nil, "", fmt.Errorf("LIST: empty stage reference")
		}
		meta, err := o.stageMgr.GetStage(db, schema, name)
		if err != nil {
			return nil, "", err
		}
		return meta, strings.ToLower(name), nil
	}
}

// qualifyIdent expands "name", "schema.name", or "db.schema.name" using the
// current session database/schema when parts are omitted.
func qualifyIdent(ref string, sess *session.Session) (db, schema, name string) {
	parts := strings.Split(ref, ".")
	switch len(parts) {
	case 3:
		return parts[0], parts[1], parts[2]
	case 2:
		return sess.Database, parts[0], parts[1]
	default:
		return sess.Database, sess.Schema, parts[0]
	}
}
