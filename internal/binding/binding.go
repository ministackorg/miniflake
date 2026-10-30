package binding

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Binding is a single bound parameter as sent by Snowflake drivers.
// Value is a JSON string for scalar binds, null for SQL NULL, and a JSON
// array for bulk (array) binds.
type Binding struct {
	Type  string          `json:"type"`
	Value json.RawMessage `json:"value"`
}

var (
	intRe     = regexp.MustCompile(`^[+-]?\d+$`)
	decimalRe = regexp.MustCompile(`^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$`)
	hexRe     = regexp.MustCompile(`^([0-9a-fA-F]{2})*$`)
)

// Substitute replaces positional (?) and numbered (:N) placeholders in sql
// with literals built from bindings, which are keyed "1", "2", ... Placeholders
// inside string literals, quoted identifiers, comments and :: casts are left
// untouched. It returns sql unchanged when bindings is empty.
func Substitute(sql string, bindings map[string]Binding) (string, error) {
	if len(bindings) == 0 {
		return sql, nil
	}
	return render(sql, func(idx int) (string, error) {
		b, ok := bindings[strconv.Itoa(idx)]
		if !ok {
			return "", fmt.Errorf("bind variable %d not set", idx)
		}
		return literal(b)
	})
}

// skipSpecial returns the index just past a string literal, quoted identifier,
// dollar-quoted string or comment starting at i.
func skipSpecial(sql string, i int) (int, bool) {
	switch c := sql[i]; {
	case c == '\'' || c == '"':
		return skipQuoted(sql, i, c), true
	case strings.HasPrefix(sql[i:], "$$"):
		return skipUntil(sql, i, "$$"), true
	case strings.HasPrefix(sql[i:], "/*"):
		return skipUntil(sql, i, "*/"), true
	case strings.HasPrefix(sql[i:], "--"), strings.HasPrefix(sql[i:], "//"):
		if j := strings.IndexByte(sql[i:], '\n'); j >= 0 {
			return i + j, true
		}
		return len(sql), true
	}
	return 0, false
}

func skipUntil(sql string, i int, end string) int {
	if k := strings.Index(sql[i+2:], end); k >= 0 {
		return i + 2 + k + len(end)
	}
	return len(sql)
}

// render walks sql and replaces every placeholder with lookup(index).
func render(sql string, lookup func(idx int) (string, error)) (string, error) {
	var out strings.Builder
	out.Grow(len(sql))
	next := 1

	for i := 0; i < len(sql); {
		if j, ok := skipSpecial(sql, i); ok {
			out.WriteString(sql[i:j])
			i = j
			continue
		}
		c := sql[i]
		switch c {
		case '?':
			lit, err := lookup(next)
			if err != nil {
				return "", err
			}
			next++
			out.WriteString(lit)
			i++
		case ':':
			if i+1 < len(sql) && sql[i+1] == ':' {
				out.WriteString("::")
				i += 2
				continue
			}
			j := i + 1
			for j < len(sql) && sql[j] >= '0' && sql[j] <= '9' {
				j++
			}
			if j == i+1 || (j < len(sql) && isIdentChar(sql[j])) || (i > 0 && isIdentChar(sql[i-1])) {
				out.WriteByte(c)
				i++
				continue
			}
			idx, _ := strconv.Atoi(sql[i+1 : j])
			lit, err := lookup(idx)
			if err != nil {
				return "", err
			}
			out.WriteString(lit)
			i = j
		default:
			out.WriteByte(c)
			i++
		}
	}
	return out.String(), nil
}

func isIdentChar(c byte) bool {
	return c == '_' || c == '$' || (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

// skipQuoted returns the index just past the quoted section starting at i.
// A doubled quote character is an escaped quote.
func skipQuoted(sql string, i int, q byte) int {
	for j := i + 1; j < len(sql); j++ {
		if sql[j] != q {
			continue
		}
		if j+1 < len(sql) && sql[j+1] == q {
			j++
			continue
		}
		return j + 1
	}
	return len(sql)
}

func quote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// literal renders a binding as a SQL literal. Every value is either validated
// against a strict pattern or quoted, so bound data can never alter the
// structure of the statement.
func literal(b Binding) (string, error) {
	raw := strings.TrimSpace(string(b.Value))
	if raw == "" || raw == "null" {
		return "NULL", nil
	}
	if raw[0] == '[' {
		return "", fmt.Errorf("array bindings are not supported")
	}
	var v string
	if err := json.Unmarshal(b.Value, &v); err != nil {
		return "", fmt.Errorf("invalid bind value for type %s", b.Type)
	}

	typ := strings.ToUpper(b.Type)
	bad := func() (string, error) {
		return "", fmt.Errorf("cannot bind value %q as %s", v, typ)
	}

	switch typ {
	case "FIXED":
		if intRe.MatchString(v) || decimalRe.MatchString(v) {
			return parenNegative(v), nil
		}
		return bad()
	case "REAL":
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return bad()
		}
		switch {
		case math.IsNaN(f):
			return "'NaN'::DOUBLE", nil
		case math.IsInf(f, 1):
			return "'Infinity'::DOUBLE", nil
		case math.IsInf(f, -1):
			return "'-Infinity'::DOUBLE", nil
		}
		return parenNegative(strconv.FormatFloat(f, 'g', -1, 64)) + "::DOUBLE", nil
	case "BOOLEAN":
		switch strings.ToLower(v) {
		case "true", "1", "t", "yes", "y", "on":
			return "TRUE", nil
		case "false", "0", "f", "no", "n", "off":
			return "FALSE", nil
		}
		return bad()
	case "DATE":
		if intRe.MatchString(v) {
			ms, _ := strconv.ParseInt(v, 10, 64)
			return "CAST(" + quote(time.UnixMilli(ms).UTC().Format("2006-01-02")) + " AS DATE)", nil
		}
		return "CAST(" + quote(v) + " AS DATE)", nil
	case "TIME":
		if intRe.MatchString(v) {
			ns, _ := strconv.ParseInt(v, 10, 64)
			t := time.Unix(0, 0).UTC().Add(time.Duration(ns))
			return "CAST(" + quote(t.Format("15:04:05.000000")) + " AS TIME)", nil
		}
		return "CAST(" + quote(v) + " AS TIME)", nil
	case "TIMESTAMP_NTZ":
		if ts, ok := epochNanos(v); ok {
			return "CAST(" + quote(ts.UTC().Format("2006-01-02 15:04:05.000000")) + " AS TIMESTAMP)", nil
		}
		return "CAST(" + quote(v) + " AS TIMESTAMP)", nil
	case "TIMESTAMP_LTZ", "TIMESTAMP_TZ":
		if ts, ok := epochNanos(v); ok {
			return "CAST(" + quote(ts.UTC().Format("2006-01-02 15:04:05.000000+00")) + " AS TIMESTAMPTZ)", nil
		}
		return "CAST(" + quote(v) + " AS TIMESTAMPTZ)", nil
	case "BINARY":
		if !hexRe.MatchString(v) {
			return bad()
		}
		return "from_hex(" + quote(v) + ")", nil
	default:
		// TEXT, ANY and unknown types bind as strings.
		return quote(v), nil
	}
}

// epochNanos parses "<nanos>" or "<nanos> <tzIndex>" as sent for timestamps.
func epochNanos(v string) (time.Time, bool) {
	field := v
	if sp := strings.IndexByte(v, ' '); sp >= 0 {
		field = v[:sp]
	}
	if !intRe.MatchString(field) {
		return time.Time{}, false
	}
	ns, err := strconv.ParseInt(field, 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	return time.Unix(0, ns), true
}

// parenNegative wraps negative numbers so "a - ?" never renders as "a --1".
func parenNegative(s string) string {
	if strings.HasPrefix(s, "-") {
		return "(" + s + ")"
	}
	return strings.TrimPrefix(s, "+")
}
