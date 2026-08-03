package orchestrator

import (
	"fmt"
	"strings"

	"github.com/miniflakedb/miniflake/internal/session"
	"github.com/miniflakedb/miniflake/internal/stage"
)

// Stage-reference resolution, shared by every statement that names a stage:
// LIST/LS, REMOVE/RM, PUT, GET and COPY INTO. Keeping one resolver here is
// what makes those commands agree on which stage a reference points at; the
// session supplies whatever the reference leaves out.
//
// Accepted forms, after the leading "@" has been stripped:
//
//	[db.][schema.]stage[/path]
//	[db.][schema.]%table[/path]
//	~[/path]

// resolveStage turns the raw text following "@" into the stage it names, the
// subpath inside that stage, and the prefix Snowflake echoes in front of file
// names for this kind of stage (see the LIST name column).
func (o *Orchestrator) resolveStage(sess *session.Session, raw string) (meta *stage.StageMeta, subpath, namePrefix string, err error) {
	ref, subpath, err := splitStageRef(cleanIdent(raw))
	if err != nil {
		return nil, "", "", err
	}
	meta, namePrefix, err = o.resolveStageRef(sess, ref)
	if err != nil {
		return nil, "", "", err
	}
	return meta, subpath, namePrefix, nil
}

// splitStageRef separates "ref[/sub/path]" into (ref, subpath).
func splitStageRef(raw string) (ref, subpath string, err error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", fmt.Errorf("empty stage reference")
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
			return nil, "", fmt.Errorf("empty table stage reference")
		}
		return o.stageMgr.GetTableStage(db, schema, table), "%" + strings.ToLower(table), nil

	default:
		db, schema, name := qualifyIdent(ref, sess)
		if name == "" {
			return nil, "", fmt.Errorf("empty stage reference")
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
