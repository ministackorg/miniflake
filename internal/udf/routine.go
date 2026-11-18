package udf

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
)

// Kind separates the two routine namespaces: Snowflake keeps functions and
// procedures apart, so a function and a procedure may share a name.
type Kind string

const (
	KindFunction  Kind = "FUNCTION"
	KindProcedure Kind = "PROCEDURE"
)

// Definition is a parsed CREATE FUNCTION / CREATE PROCEDURE statement.
type Definition struct {
	Kind        Kind
	OrReplace   bool
	IfNotExists bool
	Secure      bool
	Temporary   bool
	// NameParts is the routine name as written, unquoted: [name],
	// [schema name] or [db schema name].
	NameParts []string
	// QuotedName reports whether the last name part was double-quoted (its
	// case is then kept; otherwise Snowflake stores it upper-cased).
	QuotedName bool
	Args       []UDFArg
	ReturnType string // e.g. NUMBER, VARCHAR(10); "" for RETURNS TABLE
	// ReturnTable holds the columns of RETURNS TABLE (...), nil otherwise.
	ReturnTable []UDFArg
	Language    UDFLanguage
	Body        string
	HasBody     bool
}

// Routine is a stored function or procedure.
type Routine struct {
	Kind        Kind
	Database    string
	Schema      string
	Name        string // as stored (upper-cased unless quoted)
	Args        []UDFArg
	ReturnType  string
	ReturnTable []UDFArg
	Language    UDFLanguage
	Body        string
	Secure      bool
	CreatedAt   time.Time
}

// MinArgs is the number of arguments without a DEFAULT.
func (r *Routine) MinArgs() int {
	n := 0
	for _, a := range r.Args {
		if a.Default == "" {
			n++
		}
	}
	return n
}

// AcceptsArity reports whether a call with n arguments can bind to r.
func (r *Routine) AcceptsArity(n int) bool {
	return n >= r.MinArgs() && n <= len(r.Args)
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

var reCreateHeader = regexp.MustCompile(`(?is)^\s*CREATE\s+(OR\s+REPLACE\s+)?(SECURE\s+)?((?:TEMP|TEMPORARY|VOLATILE)\s+)?(SECURE\s+)?(FUNCTION|PROCEDURE)\s+(IF\s+NOT\s+EXISTS\s+)?`)

var (
	reReturns  = regexp.MustCompile(`(?is)\bRETURNS\s+`)
	reLanguage = regexp.MustCompile(`(?is)\bLANGUAGE\s+([A-Za-z]+)`)
	reAsQuote  = regexp.MustCompile(`(?is)\bAS\s*'`)
)

// ParseCreate parses a CREATE [OR REPLACE] [SECURE] [TEMPORARY] {FUNCTION |
// PROCEDURE} statement. The body is returned verbatim (between $$ or the
// single quotes, with ” unescaped).
func ParseCreate(sql string) (*Definition, error) {
	m := reCreateHeader.FindStringSubmatchIndex(sql)
	if m == nil {
		return nil, fmt.Errorf("not a CREATE FUNCTION or CREATE PROCEDURE statement")
	}
	d := &Definition{
		OrReplace:   m[2] >= 0,
		Secure:      m[4] >= 0 || m[8] >= 0,
		Temporary:   m[6] >= 0,
		Kind:        Kind(strings.ToUpper(sql[m[10]:m[11]])),
		IfNotExists: m[12] >= 0,
		Language:    LangSQL,
	}
	rest := sql[m[1]:]

	open := strings.IndexByte(rest, '(')
	if open < 0 {
		return nil, fmt.Errorf("missing argument list")
	}
	name := strings.TrimSpace(rest[:open])
	if name == "" {
		return nil, fmt.Errorf("missing %s name", strings.ToLower(string(d.Kind)))
	}
	d.NameParts, d.QuotedName = SplitName(name)
	closeIdx := matchingParen(rest, open)
	if closeIdx < 0 {
		return nil, fmt.Errorf("unbalanced argument list")
	}
	args, err := parseArgList(rest[open+1 : closeIdx])
	if err != nil {
		return nil, err
	}
	d.Args = args
	rest = rest[closeIdx+1:]

	// Split the clauses from the body. The body is the first $$...$$, or the
	// single-quoted string after AS.
	clauses := rest
	if i := strings.Index(rest, "$$"); i >= 0 {
		j := strings.Index(rest[i+2:], "$$")
		if j < 0 {
			return nil, fmt.Errorf("unterminated $$ body")
		}
		d.Body, d.HasBody = rest[i+2:i+2+j], true
		clauses = rest[:i]
	} else if loc := reAsQuote.FindStringIndex(rest); loc != nil {
		body, ok := readQuoted(rest[loc[1]-1:])
		if !ok {
			return nil, fmt.Errorf("unterminated quoted body")
		}
		d.Body, d.HasBody = body, true
		clauses = rest[:loc[0]]
	}

	if lm := reLanguage.FindStringSubmatch(clauses); lm != nil {
		d.Language = UDFLanguage(strings.ToUpper(lm[1]))
	}
	if rm := reReturns.FindStringIndex(clauses); rm != nil {
		after := clauses[rm[1]:]
		if strings.HasPrefix(strings.ToUpper(after), "TABLE") {
			p := strings.IndexByte(after, '(')
			if p < 0 {
				return nil, fmt.Errorf("RETURNS TABLE: missing column list")
			}
			c := matchingParen(after, p)
			if c < 0 {
				return nil, fmt.Errorf("RETURNS TABLE: unbalanced column list")
			}
			cols, err := parseArgList(after[p+1 : c])
			if err != nil {
				return nil, err
			}
			d.ReturnTable = cols
			if d.ReturnTable == nil {
				d.ReturnTable = []UDFArg{}
			}
		} else {
			d.ReturnType = readType(after)
		}
	} else {
		return nil, fmt.Errorf("missing RETURNS clause")
	}
	return d, nil
}

// readType reads a type name such as NUMBER(10, 2), VARCHAR, or DOUBLE
// PRECISION from the start of s.
func readType(s string) string {
	s = strings.TrimSpace(s)
	i := 0
	for i < len(s) && (isIdentChar(s[i])) {
		i++
	}
	typ := s[:i]
	tail := strings.TrimSpace(s[i:])
	if strings.EqualFold(typ, "DOUBLE") && strings.HasPrefix(strings.ToUpper(tail), "PRECISION") {
		return "DOUBLE PRECISION"
	}
	if strings.HasPrefix(tail, "(") {
		if c := matchingParen(tail, 0); c > 0 {
			typ += tail[:c+1]
		}
	}
	return typ
}

func isIdentChar(c byte) bool {
	return c == '_' || c == '$' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

var reDefault = regexp.MustCompile(`(?is)\s+DEFAULT\s+`)

// parseArgList parses "a NUMBER, b VARCHAR DEFAULT 'x'".
func parseArgList(s string) ([]UDFArg, error) {
	var out []UDFArg
	for _, part := range SplitTopLevel(s, ',') {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		var def string
		if loc := reDefault.FindStringIndex(part); loc != nil {
			def = strings.TrimSpace(part[loc[1]:])
			part = strings.TrimSpace(part[:loc[0]])
		}
		fields := strings.Fields(part)
		if len(fields) < 2 {
			return nil, fmt.Errorf("argument %q needs a name and a type", part)
		}
		name := fields[0]
		quoted := strings.HasPrefix(name, `"`)
		name = strings.Trim(name, `"`)
		if !quoted {
			name = strings.ToUpper(name)
		}
		out = append(out, UDFArg{
			Name:    name,
			Type:    strings.TrimSpace(strings.TrimPrefix(part, fields[0])),
			Default: def,
		})
	}
	return out, nil
}

// SplitName splits a possibly qualified, possibly quoted routine name into
// its parts. quoted reports whether the last part was quoted.
func SplitName(name string) (parts []string, quoted bool) {
	var cur strings.Builder
	inQuote, lastQuoted := false, false
	for _, r := range strings.TrimSpace(name) {
		switch {
		case r == '"':
			inQuote = !inQuote
			lastQuoted = true
		case r == '.' && !inQuote:
			parts = append(parts, cur.String())
			cur.Reset()
			lastQuoted = false
		case !inQuote && (r == ' ' || r == '\t' || r == '\n'):
		default:
			cur.WriteRune(r)
		}
	}
	return append(parts, cur.String()), lastQuoted
}

// SplitTopLevel splits s on sep where sep is outside quotes and brackets.
func SplitTopLevel(s string, sep byte) []string {
	var parts []string
	depth, start := 0, 0
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '\'', '"':
			j := i + 1
			for j < len(s) {
				if s[j] == c {
					if j+1 < len(s) && s[j+1] == c {
						j += 2
						continue
					}
					break
				}
				j++
			}
			i = j
		case '(', '[', '{':
			depth++
		case ')', ']', '}':
			depth--
		default:
			if c == sep && depth == 0 {
				parts = append(parts, s[start:i])
				start = i + 1
			}
		}
	}
	if strings.TrimSpace(s[start:]) != "" || len(parts) > 0 {
		parts = append(parts, s[start:])
	}
	return parts
}

// matchingParen returns the index of the ')' closing the '(' at open,
// skipping quoted text, or -1.
func matchingParen(s string, open int) int {
	depth := 0
	for i := open; i < len(s); i++ {
		switch c := s[i]; c {
		case '\'', '"':
			j := i + 1
			for j < len(s) {
				if s[j] == c {
					if j+1 < len(s) && s[j+1] == c {
						j += 2
						continue
					}
					break
				}
				j++
			}
			i = j
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// readQuoted reads a single-quoted string at the start of s.
func readQuoted(s string) (string, bool) {
	if s == "" || s[0] != '\'' {
		return "", false
	}
	var b strings.Builder
	for i := 1; i < len(s); i++ {
		if s[i] == '\'' {
			if i+1 < len(s) && s[i+1] == '\'' {
				b.WriteByte('\'')
				i++
				continue
			}
			return b.String(), true
		}
		b.WriteByte(s[i])
	}
	return "", false
}

// ---------------------------------------------------------------------------
// Routine registry
// ---------------------------------------------------------------------------

func routinePrefix(kind Kind, db, schema, name string) string {
	return fmt.Sprintf("%s:%s.%s.%s(", kind, strings.ToUpper(db), strings.ToUpper(schema), strings.ToUpper(name))
}

// SignatureTypes is the normalized argument type list used to tell overloads
// apart: NUMBER, VARCHAR, ...
func (r *Routine) SignatureTypes(typeName func(string) string) []string {
	out := make([]string, len(r.Args))
	for i, a := range r.Args {
		out[i] = typeName(a.Type)
	}
	return out
}

func routineKey(r *Routine, typeName func(string) string) string {
	return routinePrefix(r.Kind, r.Database, r.Schema, r.Name) + strings.Join(r.SignatureTypes(typeName), ",") + ")"
}

// PutRoutine stores r, replacing a routine with the same kind, name and
// argument types. typeName normalizes type names for the signature.
func (reg *Registry) PutRoutine(r *Routine, typeName func(string) string) {
	if r.CreatedAt.IsZero() {
		r.CreatedAt = time.Now()
	}
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if reg.routines == nil {
		reg.routines = make(map[string]*Routine)
	}
	reg.routines[routineKey(r, typeName)] = r
}

// DeleteRoutine removes r.
func (reg *Registry) DeleteRoutine(r *Routine, typeName func(string) string) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	delete(reg.routines, routineKey(r, typeName))
}

// LookupRoutines returns the routines of a kind named db.schema.name, all
// overloads, oldest first.
func (reg *Registry) LookupRoutines(kind Kind, db, schema, name string) []*Routine {
	prefix := routinePrefix(kind, db, schema, name)
	reg.mu.RLock()
	defer reg.mu.RUnlock()
	var out []*Routine
	for k, r := range reg.routines {
		if strings.HasPrefix(k, prefix) {
			out = append(out, r)
		}
	}
	sortRoutines(out)
	return out
}

// ListRoutines returns the routines of a kind in db (and schema, if not
// empty), sorted by schema and name.
func (reg *Registry) ListRoutines(kind Kind, db, schema string) []*Routine {
	reg.mu.RLock()
	defer reg.mu.RUnlock()
	var out []*Routine
	for _, r := range reg.routines {
		if r.Kind != kind {
			continue
		}
		if db != "" && !strings.EqualFold(r.Database, db) {
			continue
		}
		if schema != "" && !strings.EqualFold(r.Schema, schema) {
			continue
		}
		out = append(out, r)
	}
	sortRoutines(out)
	return out
}

func sortRoutines(rs []*Routine) {
	sort.Slice(rs, func(i, j int) bool {
		a, b := rs[i], rs[j]
		if a.Schema != b.Schema {
			return a.Schema < b.Schema
		}
		if a.Name != b.Name {
			return a.Name < b.Name
		}
		if len(a.Args) != len(b.Args) {
			return len(a.Args) < len(b.Args)
		}
		return a.CreatedAt.Before(b.CreatedAt)
	})
}
