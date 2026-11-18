// Package scripting runs the Snowflake Scripting subset MiniFlake supports
// for LANGUAGE SQL stored procedures (#37):
//
//	[DECLARE <var> [<type>] [DEFAULT <expr>]; ...]
//	BEGIN
//	  LET <var> [<type>] { DEFAULT | := } <expr>;
//	  <var> := <expr>;
//	  IF (<cond>) THEN ... [ELSEIF (<cond>) THEN ...] [ELSE ...] END IF;
//	  SELECT ... INTO :<var>[, ...] FROM ...;
//	  <any SQL statement, with :<var> binds>;
//	  BEGIN ... END;   -- nested block
//	  NULL;
//	  RETURN [<expr>];
//	END;
//
// plus the DML status variables SQLROWCOUNT, SQLFOUND and SQLNOTFOUND.
// Loops, cursors, RESULTSETs, exceptions and EXECUTE IMMEDIATE are rejected
// at parse time with an "unsupported" error.
//
// Expressions and statements are evaluated by the caller's Executor (the
// orchestrator), so they get the same rewriting and session as top-level SQL.
// Variables are substituted as typed SQL literals.
package scripting

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Result is what the Executor returns for one statement.
type Result struct {
	Columns      []string
	Types        []string // DuckDB type names, when known
	Rows         [][]interface{}
	RowsAffected int64
	IsDML        bool
}

// Executor runs one Snowflake SQL statement in the caller's session.
type Executor func(ctx context.Context, sql string) (*Result, error)

// TypeMapper maps a Snowflake type name to the DuckDB type to cast to.
type TypeMapper func(snowflakeType string) string

// Value is a variable's value and its DuckDB type ("" if unknown).
type Value struct {
	V    interface{}
	Type string
}

// UnsupportedError marks a construct outside the supported subset.
type UnsupportedError struct{ Feature string }

func (e *UnsupportedError) Error() string {
	return fmt.Sprintf("Unsupported feature '%s'.", e.Feature)
}

func unsupported(feature string) error { return &UnsupportedError{Feature: feature} }

// ---------------------------------------------------------------------------
// AST
// ---------------------------------------------------------------------------

type node interface{}

type blockNode struct{ body []node }

type assignNode struct {
	name, typ, expr string
	declare         bool // DECLARE / LET: (re)declares; assignment requires an existing var
}

type ifNode struct {
	conds    []string
	bodies   [][]node
	elseBody []node
}

type returnNode struct{ expr string }

type selectIntoNode struct {
	query string   // SELECT ... without the INTO clause
	vars  []string // target variables
}

type sqlNode struct{ text string }

type nullNode struct{}

// Procedure is a parsed procedure body.
type Procedure struct {
	root blockNode
}

// ---------------------------------------------------------------------------
// Splitting
// ---------------------------------------------------------------------------

// splitStatements splits a script on ';' outside quotes, $$ strings,
// brackets and comments. Comments are removed.
func splitStatements(src string) ([]string, error) {
	var out []string
	var cur strings.Builder
	depth := 0
	for i := 0; i < len(src); i++ {
		c := src[i]
		switch {
		case c == '-' && i+1 < len(src) && src[i+1] == '-':
			for i < len(src) && src[i] != '\n' {
				i++
			}
			cur.WriteByte(' ')
		case c == '/' && i+1 < len(src) && src[i+1] == '*':
			end := strings.Index(src[i+2:], "*/")
			if end < 0 {
				return nil, errors.New("unterminated comment")
			}
			i += end + 3
			cur.WriteByte(' ')
		case c == '\'' || c == '"':
			j := i + 1
			for ; j < len(src); j++ {
				if src[j] == c {
					if j+1 < len(src) && src[j+1] == c {
						j++
						continue
					}
					break
				}
			}
			if j >= len(src) {
				return nil, errors.New("unterminated quoted string")
			}
			cur.WriteString(src[i : j+1])
			i = j
		case c == '$' && i+1 < len(src) && src[i+1] == '$':
			end := strings.Index(src[i+2:], "$$")
			if end < 0 {
				return nil, errors.New("unterminated $$ string")
			}
			cur.WriteString(src[i : i+end+4])
			i += end + 3
		case c == '(':
			depth++
			cur.WriteByte(c)
		case c == ')':
			depth--
			cur.WriteByte(c)
		case c == ';' && depth == 0:
			out = append(out, strings.TrimSpace(cur.String()))
			cur.Reset()
		default:
			cur.WriteByte(c)
		}
	}
	if s := strings.TrimSpace(cur.String()); s != "" {
		out = append(out, s)
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Items: statements with their leading block keywords peeled off
// ---------------------------------------------------------------------------

type itemKind int

const (
	itStmt itemKind = iota
	itBegin
	itEnd
	itDeclare
	itIf
	itElseIf
	itElse
	itEndIf
)

type item struct {
	kind itemKind
	text string // statement text, or the IF / ELSEIF condition
}

var reLabel = regexp.MustCompile(`^<<\s*\w+\s*>>\s*`)

// firstWord returns the upper-cased leading word of s and the rest.
func firstWord(s string) (string, string) {
	s = strings.TrimSpace(s)
	i := 0
	for i < len(s) && (s[i] == '_' || s[i] == '$' || isAlnum(s[i])) {
		i++
	}
	return strings.ToUpper(s[:i]), strings.TrimSpace(s[i:])
}

func isAlnum(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

// findTopLevelWord returns the index of word (case-insensitive, whole word)
// in s outside quotes and brackets, or -1.
func findTopLevelWord(s, word string) int {
	depth := 0
	upper := strings.ToUpper(s)
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c == '\'' || c == '"':
			j := i + 1
			for ; j < len(s); j++ {
				if s[j] == c {
					if j+1 < len(s) && s[j+1] == c {
						j++
						continue
					}
					break
				}
			}
			i = j
		case c == '(':
			depth++
		case c == ')':
			depth--
		case depth == 0 && strings.HasPrefix(upper[i:], word):
			before := i == 0 || !(isAlnum(s[i-1]) || s[i-1] == '_')
			end := i + len(word)
			after := end >= len(s) || !(isAlnum(s[end]) || s[end] == '_')
			if before && after {
				return i
			}
		}
	}
	return -1
}

func toItems(stmts []string) ([]item, error) {
	var items []item
	for _, s := range stmts {
		for s = strings.TrimSpace(reLabel.ReplaceAllString(s, "")); s != ""; {
			w, rest := firstWord(s)
			switch w {
			case "DECLARE":
				items = append(items, item{kind: itDeclare})
				s = rest
				continue
			case "BEGIN":
				if nw, _ := firstWord(rest); nw == "TRANSACTION" || nw == "WORK" || nw == "NAME" {
					items = append(items, item{kind: itStmt, text: s})
					s = ""
					continue
				}
				items = append(items, item{kind: itBegin})
				s = rest
				continue
			case "END":
				nw, _ := firstWord(rest)
				switch nw {
				case "IF":
					items = append(items, item{kind: itEndIf})
				case "LOOP", "FOR", "WHILE", "REPEAT", "CASE":
					return nil, unsupported(nw + " statements in Snowflake Scripting")
				default: // END or END <label>
					items = append(items, item{kind: itEnd})
				}
				s = ""
				continue
			case "IF", "ELSEIF":
				then := findTopLevelWord(rest, "THEN")
				if then < 0 {
					return nil, fmt.Errorf("%s without THEN: %q", w, s)
				}
				kind := itIf
				if w == "ELSEIF" {
					kind = itElseIf
				}
				items = append(items, item{kind: kind, text: strings.TrimSpace(rest[:then])})
				s = strings.TrimSpace(rest[then+len("THEN"):])
				continue
			case "ELSE":
				items = append(items, item{kind: itElse})
				s = rest
				continue
			case "FOR", "WHILE", "LOOP", "REPEAT", "CASE", "BREAK", "CONTINUE", "EXIT", "ITERATE", "LEAVE":
				return nil, unsupported(w + " statements in Snowflake Scripting")
			case "EXCEPTION", "RAISE":
				return nil, unsupported("exception handling in Snowflake Scripting")
			case "OPEN", "FETCH", "CLOSE":
				return nil, unsupported("cursors in Snowflake Scripting")
			case "EXECUTE":
				if nw, _ := firstWord(rest); nw == "IMMEDIATE" {
					return nil, unsupported("EXECUTE IMMEDIATE")
				}
			case "AWAIT", "ASYNC":
				return nil, unsupported("asynchronous child jobs")
			}
			items = append(items, item{kind: itStmt, text: s})
			s = ""
		}
	}
	return items, nil
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

type parser struct {
	items []item
	pos   int
}

func (p *parser) peek() (item, bool) {
	if p.pos >= len(p.items) {
		return item{}, false
	}
	return p.items[p.pos], true
}

// Parse parses a procedure body (the text between the $$ delimiters).
func Parse(body string) (*Procedure, error) {
	stmts, err := splitStatements(body)
	if err != nil {
		return nil, err
	}
	items, err := toItems(stmts)
	if err != nil {
		return nil, err
	}
	p := &parser{items: items}
	blk, err := p.parseBlock()
	if err != nil {
		return nil, err
	}
	if it, ok := p.peek(); ok {
		return nil, fmt.Errorf("unexpected %s after the END of the procedure body", describe(it))
	}
	return &Procedure{root: blk}, nil
}

func describe(it item) string {
	switch it.kind {
	case itBegin:
		return "BEGIN"
	case itEnd:
		return "END"
	case itDeclare:
		return "DECLARE"
	case itIf:
		return "IF"
	case itElseIf:
		return "ELSEIF"
	case itElse:
		return "ELSE"
	case itEndIf:
		return "END IF"
	}
	return fmt.Sprintf("statement %q", it.text)
}

// parseBlock parses [DECLARE ...] BEGIN ... END.
func (p *parser) parseBlock() (blockNode, error) {
	var blk blockNode
	it, ok := p.peek()
	if !ok {
		return blk, errors.New("empty procedure body: expected BEGIN ... END")
	}
	if it.kind == itDeclare {
		p.pos++
		for {
			it, ok = p.peek()
			if !ok {
				return blk, errors.New("DECLARE section without BEGIN")
			}
			if it.kind == itBegin {
				break
			}
			if it.kind != itStmt {
				return blk, fmt.Errorf("unexpected %s in DECLARE section", describe(it))
			}
			d, err := parseDeclaration(it.text)
			if err != nil {
				return blk, err
			}
			blk.body = append(blk.body, d)
			p.pos++
		}
	}
	if it.kind != itBegin {
		return blk, fmt.Errorf("expected BEGIN, found %s", describe(it))
	}
	p.pos++
	body, term, err := p.parseNodes(itEnd)
	if err != nil {
		return blk, err
	}
	if term != itEnd {
		return blk, errors.New("BEGIN without END")
	}
	blk.body = append(blk.body, body...)
	return blk, nil
}

// parseNodes parses statements up to one of the stop items, which it
// consumes and returns.
func (p *parser) parseNodes(stops ...itemKind) ([]node, itemKind, error) {
	var out []node
	for {
		it, ok := p.peek()
		if !ok {
			return out, -1, nil
		}
		for _, s := range stops {
			if it.kind == s {
				p.pos++
				return out, it.kind, nil
			}
		}
		switch it.kind {
		case itBegin, itDeclare:
			blk, err := p.parseBlock()
			if err != nil {
				return nil, 0, err
			}
			out = append(out, blk)
		case itIf:
			n, err := p.parseIf()
			if err != nil {
				return nil, 0, err
			}
			out = append(out, n)
		case itStmt:
			n, err := parseStatement(it.text)
			if err != nil {
				return nil, 0, err
			}
			out = append(out, n)
			p.pos++
		default:
			return nil, 0, fmt.Errorf("unexpected %s", describe(it))
		}
	}
}

func (p *parser) parseIf() (node, error) {
	n := &ifNode{}
	cond := p.items[p.pos].text
	p.pos++
	for {
		body, term, err := p.parseNodes(itElseIf, itElse, itEndIf)
		if err != nil {
			return nil, err
		}
		n.conds = append(n.conds, cond)
		n.bodies = append(n.bodies, body)
		switch term {
		case itEndIf:
			return n, nil
		case itElseIf:
			cond = p.items[p.pos-1].text
		case itElse:
			elseBody, term, err := p.parseNodes(itEndIf)
			if err != nil {
				return nil, err
			}
			if term != itEndIf {
				return nil, errors.New("IF without END IF")
			}
			n.elseBody = elseBody
			return n, nil
		default:
			return nil, errors.New("IF without END IF")
		}
	}
}

var (
	reIdent     = `[A-Za-z_][A-Za-z0-9_$]*`
	reAssign    = regexp.MustCompile(`(?s)^(` + reIdent + `)\s*:=\s*(.+)$`)
	reLet       = regexp.MustCompile(`(?is)^LET\s+(` + reIdent + `)\s*(.*)$`)
	reDefaultKw = regexp.MustCompile(`(?is)^(.*?)\s*(?:\bDEFAULT\b|:=)\s*(.+)$`)
	reInto      = regexp.MustCompile(`(?is)\s+INTO\s+(:\s*` + reIdent + `(?:\s*,\s*:\s*` + reIdent + `)*)(\s+|$)`)
)

// parseDeclaration parses one entry of a DECLARE section:
// name [type] [DEFAULT expr | := expr].
func parseDeclaration(s string) (node, error) {
	w, rest := firstWord(s)
	if w == "" {
		return nil, fmt.Errorf("invalid declaration %q", s)
	}
	return declare(w, rest, s)
}

func declare(name, rest, src string) (node, error) {
	n := &assignNode{name: strings.ToUpper(name), declare: true}
	typ := rest
	if m := reDefaultKw.FindStringSubmatch(rest); m != nil {
		typ, n.expr = strings.TrimSpace(m[1]), strings.TrimSpace(m[2])
	}
	tw, _ := firstWord(typ)
	switch tw {
	case "CURSOR":
		return nil, unsupported("cursors in Snowflake Scripting")
	case "RESULTSET":
		return nil, unsupported("RESULTSET variables in Snowflake Scripting")
	case "EXCEPTION":
		return nil, unsupported("exception handling in Snowflake Scripting")
	}
	n.typ = typ
	if n.typ == "" && n.expr == "" {
		return nil, fmt.Errorf("declaration %q needs a type or a DEFAULT", src)
	}
	return n, nil
}

func parseStatement(s string) (node, error) {
	w, rest := firstWord(s)
	switch w {
	case "LET":
		m := reLet.FindStringSubmatch(s)
		if m == nil {
			return nil, fmt.Errorf("invalid LET statement %q", s)
		}
		n, err := declare(m[1], m[2], s)
		if err != nil {
			return nil, err
		}
		if n.(*assignNode).expr == "" {
			return nil, fmt.Errorf("LET %s needs DEFAULT or := <expression>", m[1])
		}
		return n, nil
	case "RETURN":
		return &returnNode{expr: rest}, nil
	case "NULL":
		if rest == "" {
			return nullNode{}, nil
		}
	case "SELECT", "WITH":
		if loc := reInto.FindStringSubmatchIndex(s); loc != nil {
			var vars []string
			for _, v := range strings.Split(s[loc[2]:loc[3]], ",") {
				vars = append(vars, strings.ToUpper(strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(v), ":"))))
			}
			query := strings.TrimSpace(s[:loc[0]] + " " + s[loc[5]:])
			return &selectIntoNode{query: query, vars: vars}, nil
		}
	}
	if m := reAssign.FindStringSubmatch(s); m != nil {
		return &assignNode{name: strings.ToUpper(m[1]), expr: strings.TrimSpace(m[2])}, nil
	}
	return &sqlNode{text: s}, nil
}

// ---------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------

type runtime struct {
	exec    Executor
	mapType TypeMapper
	vars    map[string]Value
}

// Arg is a bound procedure argument.
type Arg struct {
	Name  string
	Value Value
}

// Run executes the procedure with its bound arguments and returns the value
// of RETURN (a NULL Value when the body ends without one).
func (p *Procedure) Run(ctx context.Context, exec Executor, mapType TypeMapper, args []Arg) (Value, error) {
	rt := &runtime{exec: exec, mapType: mapType, vars: map[string]Value{}}
	for _, name := range []string{"SQLROWCOUNT", "SQLFOUND", "SQLNOTFOUND"} {
		rt.vars[name] = Value{}
	}
	for _, a := range args {
		rt.vars[strings.ToUpper(a.Name)] = a.Value
	}
	v, _, err := rt.run(ctx, p.root.body)
	return v, err
}

func (rt *runtime) run(ctx context.Context, nodes []node) (Value, bool, error) {
	for _, n := range nodes {
		if err := ctx.Err(); err != nil {
			return Value{}, false, err
		}
		switch n := n.(type) {
		case blockNode:
			if v, done, err := rt.run(ctx, n.body); done || err != nil {
				return v, done, err
			}
		case *assignNode:
			if _, ok := rt.vars[n.name]; !ok && !n.declare {
				return Value{}, false, fmt.Errorf("variable '%s' is not declared", n.name)
			}
			if n.expr == "" { // declared without DEFAULT
				rt.vars[n.name] = Value{Type: rt.mapType(n.typ)}
				continue
			}
			typ := ""
			if n.typ != "" {
				typ = rt.mapType(n.typ)
			}
			v, err := rt.eval(ctx, n.expr, typ)
			if err != nil {
				return Value{}, false, fmt.Errorf("%s: %w", n.name, err)
			}
			rt.vars[n.name] = v
		case *ifNode:
			matched := false
			for i, cond := range n.conds {
				ok, err := rt.evalBool(ctx, cond)
				if err != nil {
					return Value{}, false, err
				}
				if ok {
					matched = true
					if v, done, err := rt.run(ctx, n.bodies[i]); done || err != nil {
						return v, done, err
					}
					break
				}
			}
			if !matched && n.elseBody != nil {
				if v, done, err := rt.run(ctx, n.elseBody); done || err != nil {
					return v, done, err
				}
			}
		case *returnNode:
			if n.expr == "" {
				return Value{}, true, nil
			}
			v, err := rt.eval(ctx, n.expr, "")
			return v, true, err
		case *selectIntoNode:
			res, err := rt.exec(ctx, rt.substituteBinds(n.query))
			if err != nil {
				return Value{}, false, err
			}
			if len(res.Rows) != 1 {
				return Value{}, false, fmt.Errorf("SELECT INTO must return exactly one row, got %d", len(res.Rows))
			}
			if len(res.Columns) < len(n.vars) {
				return Value{}, false, fmt.Errorf("SELECT INTO: %d variables but %d columns", len(n.vars), len(res.Columns))
			}
			for i, name := range n.vars {
				if _, ok := rt.vars[name]; !ok {
					return Value{}, false, fmt.Errorf("variable '%s' is not declared", name)
				}
				rt.vars[name] = Value{V: res.Rows[0][i], Type: typeAt(res.Types, i)}
			}
			rt.setDMLStatus(nil)
		case *sqlNode:
			res, err := rt.exec(ctx, rt.substituteBinds(n.text))
			if err != nil {
				return Value{}, false, err
			}
			rt.setDMLStatus(res)
		case nullNode:
		}
	}
	return Value{}, false, nil
}

// setDMLStatus updates SQLROWCOUNT / SQLFOUND / SQLNOTFOUND. They describe
// the last DML statement and are NULL after any other statement.
func (rt *runtime) setDMLStatus(res *Result) {
	if res == nil || !res.IsDML {
		rt.vars["SQLROWCOUNT"], rt.vars["SQLFOUND"], rt.vars["SQLNOTFOUND"] = Value{}, Value{}, Value{}
		return
	}
	rt.vars["SQLROWCOUNT"] = Value{V: res.RowsAffected, Type: "BIGINT"}
	rt.vars["SQLFOUND"] = Value{V: res.RowsAffected > 0, Type: "BOOLEAN"}
	rt.vars["SQLNOTFOUND"] = Value{V: res.RowsAffected == 0, Type: "BOOLEAN"}
}

func typeAt(types []string, i int) string {
	if i < len(types) {
		return types[i]
	}
	return ""
}

// eval evaluates an expression (variables referenced bare or with ':') and,
// if typ is set, casts the result to it.
func (rt *runtime) eval(ctx context.Context, expr, typ string) (Value, error) {
	e := rt.substituteExpr(expr)
	if typ != "" {
		e = "CAST((" + e + ") AS " + typ + ")"
	}
	res, err := rt.exec(ctx, "SELECT "+e)
	if err != nil {
		return Value{}, err
	}
	if len(res.Rows) == 0 || len(res.Rows[0]) == 0 {
		return Value{Type: typ}, nil
	}
	return Value{V: res.Rows[0][0], Type: typeAt(res.Types, 0)}, nil
}

func (rt *runtime) evalBool(ctx context.Context, cond string) (bool, error) {
	res, err := rt.exec(ctx, "SELECT CASE WHEN ("+rt.substituteExpr(cond)+") THEN TRUE ELSE FALSE END")
	if err != nil {
		return false, err
	}
	if len(res.Rows) == 0 || len(res.Rows[0]) == 0 {
		return false, nil
	}
	b, _ := res.Rows[0][0].(bool)
	return b, nil
}

// ---------------------------------------------------------------------------
// Variable substitution
// ---------------------------------------------------------------------------

// scanCode calls emit for every run of plain code in s and copies quoted
// strings, quoted identifiers and $$ strings through unchanged.
func scanCode(s string, emit func(code string) string) string {
	var out strings.Builder
	start := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		var end int
		switch {
		case c == '\'' || c == '"':
			end = i + 1
			for ; end < len(s); end++ {
				if s[end] == c {
					if end+1 < len(s) && s[end+1] == c {
						end++
						continue
					}
					break
				}
			}
		case c == '$' && i+1 < len(s) && s[i+1] == '$':
			e := strings.Index(s[i+2:], "$$")
			if e < 0 {
				end = len(s) - 1
			} else {
				end = i + e + 3
			}
		default:
			continue
		}
		if end >= len(s) {
			end = len(s) - 1
		}
		out.WriteString(emit(s[start:i]))
		out.WriteString(s[i : end+1])
		start = end + 1
		i = end
	}
	out.WriteString(emit(s[start:]))
	return out.String()
}

var reBind = regexp.MustCompile(`:(` + reIdent + `)`)
var reWord = regexp.MustCompile(reIdent)

// substituteBinds replaces :var in a SQL statement with the variable's
// literal. `a::INT` casts and `col:path` VARIANT access are left alone, as
// are names that are not variables.
func (rt *runtime) substituteBinds(sql string) string {
	return scanCode(sql, func(code string) string {
		var out strings.Builder
		last := 0
		for _, loc := range reBind.FindAllStringSubmatchIndex(code, -1) {
			if loc[0] > 0 {
				prev := code[loc[0]-1]
				if prev == ':' || prev == '_' || prev == '$' || isAlnum(prev) {
					continue
				}
			}
			v, ok := rt.vars[strings.ToUpper(code[loc[2]:loc[3]])]
			if !ok {
				continue
			}
			out.WriteString(code[last:loc[0]])
			out.WriteString(Literal(v))
			last = loc[1]
		}
		out.WriteString(code[last:])
		return out.String()
	})
}

// substituteExpr replaces variables in a Scripting expression, where they
// are referenced without a colon (RETURN x + 1) or with one.
func (rt *runtime) substituteExpr(expr string) string {
	expr = rt.substituteBinds(expr)
	return scanCode(expr, func(code string) string {
		var out strings.Builder
		last := 0
		for _, loc := range reWord.FindAllStringIndex(code, -1) {
			if loc[0] > 0 {
				prev := code[loc[0]-1]
				if prev == '.' || prev == ':' || prev == '_' || prev == '$' || isAlnum(prev) {
					continue
				}
			}
			next := strings.TrimLeft(code[loc[1]:], " \t\r\n")
			if strings.HasPrefix(next, "(") || strings.HasPrefix(next, ".") {
				continue // function call or qualified name
			}
			v, ok := rt.vars[strings.ToUpper(code[loc[0]:loc[1]])]
			if !ok {
				continue
			}
			out.WriteString(code[last:loc[0]])
			out.WriteString(Literal(v))
			last = loc[1]
		}
		out.WriteString(code[last:])
		return out.String()
	})
}

// ---------------------------------------------------------------------------
// Literals
// ---------------------------------------------------------------------------

func quote(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// Literal renders a value as a DuckDB SQL literal of its type.
func Literal(v Value) string {
	typ := strings.ToUpper(v.Type)
	if typ == "SQLNULL" || typ == "NULL" || typ == "INVALID" {
		typ = "" // the type of a bare NULL literal; not castable
	}
	if v.V == nil {
		if typ != "" {
			return "CAST(NULL AS " + typ + ")"
		}
		return "NULL"
	}
	switch x := v.V.(type) {
	case string:
		switch {
		case typ == "JSON":
			return "CAST(" + quote(x) + " AS JSON)"
		case typ != "" && typ != "VARCHAR":
			return "CAST(" + quote(x) + " AS " + typ + ")"
		}
		return quote(x)
	case bool:
		if x {
			return "TRUE"
		}
		return "FALSE"
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", x)
	case float32:
		return "CAST(" + strconv.FormatFloat(float64(x), 'g', -1, 32) + " AS DOUBLE)"
	case float64:
		return "CAST(" + strconv.FormatFloat(x, 'g', -1, 64) + " AS DOUBLE)"
	case time.Time:
		switch {
		case typ == "DATE":
			return "DATE '" + x.Format("2006-01-02") + "'"
		case typ == "TIME" || strings.HasPrefix(typ, "TIME("):
			return "TIME '" + x.Format("15:04:05.999999") + "'"
		case typ == "TIMESTAMPTZ" || typ == "TIMESTAMP WITH TIME ZONE":
			return "TIMESTAMPTZ '" + x.UTC().Format("2006-01-02 15:04:05.999999") + "+00'"
		}
		return "TIMESTAMP '" + x.Format("2006-01-02 15:04:05.999999") + "'"
	case []byte:
		return fmt.Sprintf("from_hex('%x')", x)
	case fmt.Stringer: // duckdb.Decimal, UUID, ...
		if typ != "" {
			return "CAST(" + quote(x.String()) + " AS " + typ + ")"
		}
		return quote(x.String())
	}
	b, err := json.Marshal(v.V)
	if err != nil {
		return quote(fmt.Sprint(v.V))
	}
	return "CAST(" + quote(string(b)) + " AS JSON)"
}
